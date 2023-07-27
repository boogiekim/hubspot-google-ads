from google.ads.googleads.client import GoogleAdsClient

def check_job_status(customer_id, job_resource_name):
    client = GoogleAdsClient.load_from_storage("/Users/johnkim/google-ads-python/google-ads-test-client.yaml")
    query = (
        f"SELECT offline_user_data_job.resource_name, offline_user_data_job.id, "
        f"offline_user_data_job.status, offline_user_data_job.type, "
        f"offline_user_data_job.failure_reason, offline_user_data_job.customer_match_user_list_metadata.user_list "
        f"FROM offline_user_data_job "
        f"WHERE offline_user_data_job.resource_name = '{job_resource_name}' "
        f"LIMIT 1"
    )

    try:
        google_ads_service = client.service.google_ads
        response = google_ads_service.search(query=query, customer_id=customer_id)
        for row in response:
            print("Job Resource Name:", row.offline_user_data_job.resource_name)
            print("Job ID:", row.offline_user_data_job.id.value)
            print("Job Status:", row.offline_user_data_job.status)
            print("Job Type:", row.offline_user_data_job.type)
            print("Failure Reason:", row.offline_user_data_job.failure_reason)
            print("Customer Match User List:", row.offline_user_data_job.customer_match_user_list_metadata.user_list)
    except Exception as e:
        print(f"Error occurred: {e}")

# Replace with your actual customer ID and job resource name
customer_id = 8665802929
job_resource_name = "customers/8665802929/offlineUserDataJobs/32350238744"

check_job_status(customer_id, job_resource_name)
