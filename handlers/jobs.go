package handlers

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/glog"

	batchv1 "k8s.io/api/batch/v1"
	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	batchtypev1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

var (
	trueVal  = true
	falseVal = false
)

const (
	appLabel = "ssjdispatcherjob"
)

type JobsArray struct {
	JobInfo []JobInfo `json:"jobs"`
}

type jobHandler struct {
	jobClient batchtypev1.JobInterface
}

func NewJobHandler() *jobHandler {
	return &jobHandler{
		jobClient: getJobClient(),
	}
}

type JobInfo struct {
	UID        string `json:"uid"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	URL        string `json:"url"`
	jobStatus  *batchv1.JobStatus
	SQSMessage *sqs.Message
}

func (j *JobInfo) DetailedStatus() string {
	return fmt.Sprintf("Succeeded:%d - Failed:%d - Active:%d - Started:%v - Completed:%v", j.jobStatus.Succeeded, j.jobStatus.Failed, j.jobStatus.Failed, j.jobStatus.StartTime, j.jobStatus.CompletionTime)
}

func getJobClient() batchtypev1.JobInterface {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	// Access jobs. We can't do it all in one line, since we need to receive the
	// errors and manage thgem appropriately
	batchClient := clientset.BatchV1()
	jobsClient := batchClient.Jobs(os.Getenv("GEN3_NAMESPACE"))
	return jobsClient
}

func (h *jobHandler) getJobByID(jobId string) (*batchv1.Job, error) {
	job, err := h.jobClient.Get(jobId, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return job, nil
}

//GetJobStatusByID returns job status given job id
func (h *jobHandler) GetJobStatusByID(jobid string) (*JobInfo, error) {
	job, err := h.getJobByID(jobid)
	if err != nil {
		return nil, err
	}
	ji := JobInfo{}
	ji.Name = job.Name
	ji.UID = string(job.GetUID())
	ji.Status = jobStatusToString(&job.Status)
	return &ji, nil
}

// deleteJobByName deletes a job along with its dependencies by job name.
func (h *jobHandler) deleteJobByName(jobName string, afterSeconds int64) error {
	deleteOption := metav1.NewDeleteOptions(afterSeconds)

	var deletionPropagation metav1.DeletionPropagation = "Background"
	deleteOption.PropagationPolicy = &deletionPropagation

	if err := h.jobClient.Delete(jobName, deleteOption); err != nil {
		glog.Errorf("[deleteJobByName] error deleting job %s: %s", jobName, err)
		return err
	}

	return nil
}

func (h *jobHandler) listJobs() JobsArray {
	jobs := JobsArray{}

	labelSelector := labels.SelectorFromSet(map[string]string{"app": appLabel}).String()

	glog.Infof("[listJobs] list all jobs with label %q", labelSelector)

	jobsList, err := h.jobClient.List(metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		glog.Errorf("[listJobs] error listing jobs: %s", err)
		return jobs
	}

	for _, job := range jobsList.Items {
		jobs.JobInfo = append(jobs.JobInfo, JobInfo{
			Name:      job.Name,
			UID:       string(job.GetUID()),
			Status:    jobStatusToString(&job.Status),
			jobStatus: &job.Status,
		})
	}

	return jobs
}

func jobStatusToString(status *batchv1.JobStatus) string {
	if status == nil {
		return "Unknown"
	}

	// https://kubernetes.io/docs/api-reference/batch/v1/definitions/#_v1_jobstatus
	if status.Succeeded >= 1 {
		return "Completed"
	}
	if status.Failed >= 1 {
		return "Failed"
	}
	if status.Active >= 1 {
		return "Running"
	}
	return "Unknown"
}

// RemoveCompletedJobs removes all completed k8s jobs dispatched by the service
func (h *jobHandler) RemoveCompletedJobs() []string {
	jobs := h.listJobs()
	var deletedJobs []string
	for _, job := range jobs.JobInfo {
		glog.Infof("[RemoveCompletedJobs] check to remove job %s: %q [%s]", job.Name, job.Status, job.DetailedStatus())

		isCompleted := job.Status == "Completed"
		isFailedAndExceededRetries := job.Status == "Failed" && job.jobStatus.Failed >= int32(MAX_RETRIES)

		if isCompleted || isFailedAndExceededRetries {
			glog.Infof("[RemoveCompletedJobs] removing job %s with %d second(s) grace period", job.Name, GRACE_PERIOD)
			if err := h.deleteJobByName(job.Name, GRACE_PERIOD); err != nil {
				glog.Errorf("[RemoveCompletedJobs] error deleting job %s: %s", job.Name, err)
			} else {
				deletedJobs = append(deletedJobs, job.UID)
			}
		}
	}
	return deletedJobs
}

// GetNumberRunningJobs returns number of k8s running jobs dispatched by the service
func (h *jobHandler) GetNumberRunningJobs() int {
	jobs := h.listJobs()
	nRunningJobs := 0
	for i := 0; i < len(jobs.JobInfo); i++ {
		job := jobs.JobInfo[i]
		if job.Status == "Running" || job.Status == "Unknown" {
			nRunningJobs++
		}
	}
	return nRunningJobs
}

// CreateK8sJob creates a k8s job to handle s3 object
func CreateK8sJob(inputURL string, jobConfig JobConfig) (*JobInfo, error) {
	// Skip all checking errors since aws cred file was properly loaded already
	credBytes, _ := ReadFile(LookupCredFile())
	regionIf, _ := GetValueFromJSON(credBytes, []string{"AWS", "region"})
	accessKeyIf, _ := GetValueFromJSON(credBytes, []string{"AWS", "aws_access_key_id"})
	secretKeyIf, _ := GetValueFromJSON(credBytes, []string{"AWS", "aws_secret_access_key"})

	awsRegion := ""
	if regionIf != nil {
		awsRegion = regionIf.(string)
	}
	awsAccessKey := ""
	if accessKeyIf != nil {
		awsAccessKey = accessKeyIf.(string)
	}
	awsSecretKey := ""
	if secretKeyIf != nil {
		awsSecretKey = secretKeyIf.(string)
	}

	configBytes, err := json.Marshal(jobConfig.ImageConfig)
	if err != nil {
		return nil, err
	}

	configString := string(configBytes)

	jobsClient := getJobClient()
	randname := GetRandString(5)
	name := fmt.Sprintf("%s-%s", jobConfig.Name, randname)
	glog.Infof("job input URL: %s", inputURL)
	var deadline int64 = 72000

	labels := make(map[string]string)
	labels["app"] = appLabel

	if jobConfig.RequestCPU == "" {
		jobConfig.RequestCPU = "500m"
	}

	if jobConfig.RequestMem == "" {
		jobConfig.RequestMem = "0.1Gi"
	}

	if jobConfig.DeadLine != 0 {
		deadline = jobConfig.DeadLine
	}

	glog.Info("Deadline: ", deadline)

	quayImage := jobConfig.Image
	val, ok := os.LookupEnv("JOB_IMAGES")
	if ok {
		quayImageIf, err := GetValueFromJSON([]byte(val), []string{jobConfig.Name})
		if err != nil {
			return nil, err
		}
		quayImage = quayImageIf.(string)
	}

	serviceAccount := "ssjdispatcher-job-sa"
	if jobConfig.ServiceAccount != "" {
		serviceAccount = jobConfig.ServiceAccount
	}
	glog.Info("Job service account: ", serviceAccount)

	// For an example of how to create jobs, see this file:
	// https://github.com/pachyderm/pachyderm/blob/805e63/src/server/pps/server/api_server.go#L2320-L2345
	batchJob := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: batchv1.JobSpec{
			// Optional: Parallelism:,
			// Optional: Completions:,
			// Optional: ActiveDeadlineSeconds:,
			// Optional: Selector:,
			// Optional: ManualSelector:,
			BackoffLimit:          aws.Int32(int32(MAX_RETRIES)),
			ActiveDeadlineSeconds: &deadline,
			Template: k8sv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: k8sv1.PodSpec{
					InitContainers:     []k8sv1.Container{}, // Doesn't seem obligatory(?)...
					ServiceAccountName: serviceAccount,
					Containers: []k8sv1.Container{
						{
							Name:  "job-task",
							Image: quayImage,
							SecurityContext: &k8sv1.SecurityContext{
								Privileged: &falseVal,
							},
							ImagePullPolicy: k8sv1.PullPolicy(k8sv1.PullAlways),
							Resources: k8sv1.ResourceRequirements{
								Requests: k8sv1.ResourceList{
									k8sv1.ResourceCPU:    resource.MustParse(jobConfig.RequestCPU),
									k8sv1.ResourceMemory: resource.MustParse(jobConfig.RequestMem),
								},
							},
							Env: []k8sv1.EnvVar{
								{
									Name:  "INPUT_URL",
									Value: inputURL,
								},
								{
									Name:  "AWS_REGION",
									Value: awsRegion,
								},
								{
									Name:  "AWS_ACCESS_KEY_ID",
									Value: awsAccessKey,
								},
								{
									Name:  "AWS_SECRET_ACCESS_KEY",
									Value: awsSecretKey,
								},
								{
									Name:  "CONFIG_FILE",
									Value: configString,
								},
							},
							VolumeMounts: []k8sv1.VolumeMount{},
						},
					},
					RestartPolicy:    k8sv1.RestartPolicyNever,
					Volumes:          []k8sv1.Volume{},
					ImagePullSecrets: []k8sv1.LocalObjectReference{},
				},
			},
		},
		// Optional, not used by pach: JobStatus:,
	}

	newJob, err := jobsClient.Create(batchJob)
	if err != nil {
		return nil, err
	}
	glog.Infof("[CreateK8sJob] new job name: %q", newJob.Name)
	ji := JobInfo{
		Name:      newJob.Name,
		UID:       string(newJob.GetUID()),
		URL:       inputURL,
		Status:    jobStatusToString(&newJob.Status),
		jobStatus: &newJob.Status,
	}

	return &ji, nil
}
