gcloud builds submit ./src --tag gcr.io/any-development/test-cloud-run-job

gcloud run jobs create export-bq-to-gcs-job \
  --image=gcr.io/any-development/test-cloud-run-job \
  --memory=512Mi \
  --region=us-central1 \
  --service-account=cloud-run-jobs-sa \
  --set-env-vars=PROJECT_ID=any-development,SOURCE_DATASET=TEST_DATA,DESTINATION_BUCKET=test-cloud-run-jobs \
  --tasks 30

gcloud run jobs update export-bq-to-gcs-job \
  --image=gcr.io/any-development/test-cloud-run-job \
  --memory=512Mi \
  --region=us-central1 \
  --service-account=cloud-run-jobs-sa \
  --set-env-vars=PROJECT_ID=any-development,SOURCE_DATASET=TEST_DATA,DESTINATION_BUCKET=test-cloud-run-jobs \
  --tasks 30

