# CellStop Gundi Repository

## Description

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec nunc eu mi scelerisque porttitor ac at nibh. Fusce efficitur erat risus, ut euismod velit lacinia at. Aenean quis gravida elit. Aliquam erat volutpat. Duis sagittis dictum est, nec malesuada nisi feugiat non. Interdum et malesuada fames ac ante ipsum primis in faucibus. Maecenas convallis fringilla lectus, eu varius nisl dignissim vitae.


## How to test

- Lorem ipsum dolor sit amet, consectetur adipiscing elit.
- Curabitur porttitor velit vitae malesuada sodales.
- Integer efficitur risus a nisi finibus, in sagittis risus fringilla.
- Curabitur dictum eros id libero sodales hendrerit.

## How to deploy

### STEPS:
#### First deploy:
- Run `./first-deploy.sh <ENV>` in the root folder.

    - **NOTE:** If permission denied error returned, run `chmod +x first-deploy.sh` in the root folder and try again.

    - **NOTE:** A file named `.env.<ENV>` needs to be present in the `/src` folder with the needed variables for running in selected ENV for this to work as expected.


- Check the logs to verify everything is ok.


- Check on:

    - [GCP Cloud Run jobs dashboard](https://console.cloud.google.com/run/jobs?project=cdip-78ca) to verify the job was created successfully.
    - [Jobs schedules](https://console.cloud.google.com/cloudscheduler?project=cdip-78ca) to verify the trigger.
    - [Secret Manager](https://console.cloud.google.com/security/secret-manager?project=cdip-78ca) to verify the secret.


#### Update integration
- Run `gcloud builds submit --config=cloudbuild.yaml --substitutions=_INTEGRATION=cellstop .` in the root folder, pointing the branch with the changes to be applied in the integration.


- Check the logs to verify everything is ok.
