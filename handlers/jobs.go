package handlers

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	batchtypev1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

var (
	trueVal  = true
	falseVal = false
)

type JobsArray struct {
	JobInfo []JobInfo `json:"jobs"`
}

type JobInfo struct {
	UID    string `json:"uid"`
	Name   string `json:"name"`
	Status string `json:"status"`
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

func getJobByID(jc batchtypev1.JobInterface, jobid string) (*batchv1.Job, error) {
	jobs, err := jc.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, job := range jobs.Items {
		if jobid == string(job.GetUID()) {
			return &job, nil
		}
	}
	return nil, fmt.Errorf("job with jobid %s not found", jobid)
}

func getJobStatusByID(jobid string) (*JobInfo, error) {
	job, err := getJobByID(getJobClient(), jobid)
	if err != nil {
		return nil, err
	}
	ji := JobInfo{}
	ji.Name = job.Name
	ji.UID = string(job.GetUID())
	ji.Status = jobStatusToString(&job.Status)
	return &ji, nil
}

// delete job along with dependencies by job id
// it should be called when job's status is completed
func deleteJobByID(jobid string, afterSeconds int64) error {

	client := getJobClient()
	job, err := getJobByID(client, jobid)
	if err != nil {
		return err
	}

	deleteOption := metav1.NewDeleteOptions(afterSeconds)

	var deletionPropagation metav1.DeletionPropagation = "Background"
	deleteOption.PropagationPolicy = &deletionPropagation

	if err = client.Delete(job.Name, deleteOption); err != nil {
		fmt.Println(err)
	}

	return nil

}

func listJobs(jc batchtypev1.JobInterface) JobsArray {
	jobs := JobsArray{}

	jobsList, err := jc.List(metav1.ListOptions{})

	if err != nil {
		return jobs
	}

	for _, job := range jobsList.Items {
		ji := JobInfo{}
		ji.Name = job.Name
		ji.UID = string(job.GetUID())
		ji.Status = jobStatusToString(&job.Status)
		jobs.JobInfo = append(jobs.JobInfo, ji)
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

func isResourceAvailbility(prefix string) bool {

	jobs := listJobs(getJobClient())
	nJobRunning := 0
	for i := 0; i < len(jobs.JobInfo); i++ {
		job := jobs.JobInfo[i]
		if strings.HasPrefix(job.Name, prefix) {
			if job.Status == "Running" {
				nJobRunning++
			} else if job.Status == "Completed" {
				deleteJobByID(job.UID, 1)
			}
		}
	}
	maxJobNum, err := strconv.Atoi(os.Getenv("JOB_NUM_MAX"))
	if err != nil {
		log.Panicln("JOB_NUM_MAX env is not set corectly")
	}
	return nJobRunning < maxJobNum

}

func createK8sJob(inputURL string, jobConfig JobConfig) (*JobInfo, error) {
	// if number of running jobs is larger than predefined JOB_NUM_MAX, return error
	// so that the inputURL is put back to SQS.
	if !isResourceAvailbility(jobConfig.Name) {
		return nil, errors.New("Number of running jobs is bigger than JOB_NUM_MAX")
	}

	// Skip all checking errors since aws cred file was properly loaded already
	credBytes, _ := ReadFile(Lookup_cred_file())
	regionIf, _ := GetValueFromJson(credBytes, []string{"AWS", "region"})
	accessKeyIf, _ := GetValueFromJson(credBytes, []string{"AWS", "aws_access_key_id"})
	secretKeyIf, _ := GetValueFromJson(credBytes, []string{"AWS", "aws_secret_access_key"})

	configBytes, err := json.Marshal(jobConfig.ImageConfig)
	if err != nil {
		return nil, err
	}

	configString := string(configBytes)

	jobsClient := getJobClient()
	randname, _ := GetRandString(5)
	name := fmt.Sprintf("%s-%s", jobConfig.Name, randname)
	fmt.Println("job input URL: ", inputURL)
	var deadline int64 = 600
	labels := make(map[string]string)
	labels["app"] = "ssjdispatcher"
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
			ActiveDeadlineSeconds: &deadline,
			Template: k8sv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: k8sv1.PodSpec{
					InitContainers: []k8sv1.Container{}, // Doesn't seem obligatory(?)...
					Containers: []k8sv1.Container{
						{
							Name:  "job-task",
							Image: jobConfig.Image,
							SecurityContext: &k8sv1.SecurityContext{
								Privileged: &falseVal,
							},
							ImagePullPolicy: k8sv1.PullPolicy(k8sv1.PullAlways),
							Env: []k8sv1.EnvVar{
								{
									Name:  "INPUT_URL",
									Value: inputURL,
								},
								{
									Name:  "AWS_REGION",
									Value: regionIf.(string),
								},
								{
									Name:  "AWS_ACCESS_KEY_ID",
									Value: accessKeyIf.(string),
								},
								{
									Name:  "AWS_SECRET_ACCESS_KEY",
									Value: secretKeyIf.(string),
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
	fmt.Println("New job name: ", newJob.Name)
	ji := JobInfo{}
	ji.Name = newJob.Name
	ji.UID = string(newJob.GetUID())
	ji.Status = jobStatusToString(&newJob.Status)
	return &ji, nil
}

// GetRandString returns a random string of lenght N
func GetRandString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.ToLower(base64.RawURLEncoding.EncodeToString(b)), nil
}
