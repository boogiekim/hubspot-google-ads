import json
import requests
import hashlib
import datetime
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def lambda_handler(event):
    print("At least the lambda_handler has been called...")
    customer_id = event.get('customer_id')
    run_job = event.get('run_job')
    user_list_id = event.get('user_list_id')
    offline_user_data_job_id = event.get('offline_user_data_job_id')
    
    developer_token = os.environ['developer_token']
    use_proto_plus = os.environ['use_proto_plus']
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    refresh_token = os.environ['refresh_token']
    login_customer_id = os.environ['login_customer_id']

    credentials = {
        "developer_token": developer_token,
        "use_proto_plus": use_proto_plus,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "login_customer_id": login_customer_id
    }

    googleads_client = GoogleAdsClient.load_from_dict(credentials)
    
    try:
        main(
            googleads_client,
            customer_id,
            run_job,
            user_list_id,
            offline_user_data_job_id,
        )
    except GoogleAdsException as ex:
        print(
            f"Request with ID '{ex.request_id}' failed with status "
            f"'{ex.error.code().name}' and includes the following errors:"
        )
        for error in ex.failure.errors:
            print(f"\tError with message '{error.message}'.")
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        print(error.message)
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred while processing the Google Ads request.')
        }
    
    print("At least lambda_handler was executed....")
    return {
        'statusCode': 200,
        'body': json.dumps('Ads sync processed with no errors.')
    }

def main(client, customer_id, run_job, user_list_id, offline_user_data_job_id):

    googleads_service = client.get_service("GoogleAdsService")

    if not offline_user_data_job_id:
        if user_list_id:
            # Uses the specified Customer Match user list.
            user_list_resource_name = googleads_service.user_list_path(
                customer_id, user_list_id
            )
        else:
            # Creates a Customer Match user list.
            user_list_resource_name = create_customer_match_user_list(
                client, customer_id
            )

    add_users_to_customer_match_user_list(
        client,
        customer_id,
        user_list_resource_name,
        run_job,
        offline_user_data_job_id,
    )

    
# [START add_customer_match_user_list_3]
def create_customer_match_user_list(client, customer_id):
    # Creates the UserListService client.
    user_list_service_client = client.get_service("UserListService")

    # Creates the user list operation.
    user_list_operation = client.get_type("UserListOperation")

    # Creates the new user list.
    user_list = user_list_operation.create
    current_date = datetime.datetime.now()
    user_list.name = f"lambda-function-testing-list-{current_date.strftime('%Y%m%d_%H%M%S')}"
    user_list.description = (
        "A list of customers that originated from email and physical addresses"
    )
    # Sets the upload key type to indicate the type of identifier that is used
    # to add users to the list. This field is immutable and required for a
    # CREATE operation.
    user_list.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )

    user_list.membership_life_span = 30

    response = user_list_service_client.mutate_user_lists(
        customer_id=customer_id, operations=[user_list_operation]
    )
    user_list_resource_name = response.results[0].resource_name
    print(
        f"User list with resource name '{user_list_resource_name}' was created."
    )

    return user_list_resource_name
    # [END add_customer_match_user_list_3]


# [START add_customer_match_user_list]
def add_users_to_customer_match_user_list(
    client,
    customer_id,
    user_list_resource_name,
    run_job,
    offline_user_data_job_id,
):

    # Creates the OfflineUserDataJobService client.
    offline_user_data_job_service_client = client.get_service(
        "OfflineUserDataJobService"
    )

    if offline_user_data_job_id:
        # Reuses the specified offline user data job.
        offline_user_data_job_resource_name = offline_user_data_job_service_client.offline_user_data_job_path(
            customer_id, offline_user_data_job_id
        )
    else:
        # Creates a new offline user data job.
        offline_user_data_job = client.get_type("OfflineUserDataJob")
        offline_user_data_job.type_ = (
            client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
        )
        offline_user_data_job.customer_match_user_list_metadata.user_list = (
            user_list_resource_name
        )
        # Issues a request to create an offline user data job.
        create_offline_user_data_job_response = offline_user_data_job_service_client.create_offline_user_data_job(
            customer_id=customer_id, job=offline_user_data_job
        )
        offline_user_data_job_resource_name = (
            create_offline_user_data_job_response.resource_name
        )
        print(
            "Created an offline user data job with resource name: "
            f"'{offline_user_data_job_resource_name}'."
        )

    # Issues a request to add the operations to the offline user data job.

    request = client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = offline_user_data_job_resource_name
    request.operations = build_offline_user_data_job_operations(client)
    request.enable_partial_failure = True

    # Issues a request to add the operations to the offline user data job.
    response = offline_user_data_job_service_client.add_offline_user_data_job_operations(
        request=request
    )

    # Prints the status message if any partial failure error is returned.
    # Note: the details of each partial failure error are not printed here.
    # Refer to the error_handling/handle_partial_failure.py example to learn
    # more.
    # Extracts the partial failure from the response status.
    partial_failure = getattr(response, "partial_failure_error", None)
    if getattr(partial_failure, "code", None) != 0:
        error_details = getattr(partial_failure, "details", [])
        for error_detail in error_details:
            failure_message = client.get_type("GoogleAdsFailure")
            # Retrieve the class definition of the GoogleAdsFailure instance
            # in order to use the "deserialize" class method to parse the
            # error_detail string into a protobuf message object.
            failure_object = type(failure_message).deserialize(
                error_detail.value
            )

            for error in failure_object.errors:
                print(
                    "A partial failure at index "
                    f"{error.location.field_path_elements[0].index} occurred.\n"
                    f"Error message: {error.message}\n"
                    f"Error code: {error.error_code}"
                )

    print("The operations are added to the offline user data job.")

    if not run_job:
        print(
            "Not running offline user data job "
            f"'{offline_user_data_job_resource_name}', as requested."
        )
        return

    # Issues a request to run the offline user data job for executing all
    # added operations.
    offline_user_data_job_service_client.run_offline_user_data_job(
        resource_name=offline_user_data_job_resource_name
    )

    # Retrieves and displays the job status.
    check_job_status(client, customer_id, offline_user_data_job_resource_name)
    # [END add_customer_match_user_list]


# [START add_customer_match_user_list_2]
def build_offline_user_data_job_operations(client):
    # Adds the raw records to a raw input list.
    raw_records = []
    api_url = "https://api.hubapi.com/contacts/v1/lists/30/contacts/all?property=firstname&property=lastname&property=email&property=phone"

    headers = {
        'Authorization': 'Bearer pat-na1-111e57ab-b515-4a74-9c35-d7a64c10aa0e'
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        for x in data["contacts"]:
            raw_record = {
                "first_name": x["properties"]["firstname"]["value"],
                "last_name": x["properties"]["lastname"]["value"],
            }
            if "phone" in x["properties"]:
                raw_record["phone"] = x["properties"]["phone"]["value"]
            if "email" in x["properties"]:
                raw_record["email"] = x["properties"]["email"]["value"]
            raw_records.append(raw_record)
            print(raw_record)
    else:
        print(f"Request failed with status code: {response.status_code}")



    operations = []
    # Iterates over the raw input list and creates a UserData object for each
    # record.
    for record in raw_records:
        # Creates a UserData object that represents a member of the user list.
        user_data = client.get_type("UserData")

        
        if "email" in record:
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_email = normalize_and_hash(
                record["email"], True
            )
            # Adds the hashed email identifier to the UserData object's list.
            user_data.user_identifiers.append(user_identifier)

        # Checks if the record has a phone number, and if so, adds a
        # UserIdentifier for it.
        if "phone" in record:
            user_identifier = client.get_type("UserIdentifier")
            user_identifier.hashed_phone_number = normalize_and_hash(
                record["phone"], True
            )
            # Adds the hashed phone number identifier to the UserData object's
            # list.
            user_data.user_identifiers.append(user_identifier)

        # If the user_identifiers repeated field is not empty, create a new
        # OfflineUserDataJobOperation and add the UserData to it.
        if user_data.user_identifiers:
            operation = client.get_type("OfflineUserDataJobOperation")
            operation.create = user_data
            operations.append(operation)
        # [END add_customer_match_user_list_2]

    return operations


# [START add_customer_match_user_list_4]
def check_job_status(client, customer_id, offline_user_data_job_resource_name):

    query = f"""
        SELECT
          offline_user_data_job.resource_name,
          offline_user_data_job.id,
          offline_user_data_job.status,
          offline_user_data_job.type,
          offline_user_data_job.failure_reason,
          offline_user_data_job.customer_match_user_list_metadata.user_list
        FROM offline_user_data_job
        WHERE offline_user_data_job.resource_name =
          '{offline_user_data_job_resource_name}'
        LIMIT 1"""

    # Issues a search request using streaming.
    google_ads_service = client.get_service("GoogleAdsService")
    results = google_ads_service.search(customer_id=customer_id, query=query)
    offline_user_data_job = next(iter(results)).offline_user_data_job
    status_name = offline_user_data_job.status.name
    user_list_resource_name = (
        offline_user_data_job.customer_match_user_list_metadata.user_list
    )

    print(
        f"Offline user data job ID '{offline_user_data_job.id}' with type "
        f"'{offline_user_data_job.type_.name}' has status: {status_name}"
    )

    if status_name == "SUCCESS":
        print_customer_match_user_list_info(
            client, customer_id, user_list_resource_name
        )
    elif status_name == "FAILED":
        print(f"\tFailure Reason: {offline_user_data_job.failure_reason}")
    elif status_name in ("PENDING", "RUNNING"):
        print(
            "To check the status of the job periodically, use the following "
            f"GAQL query with GoogleAdsService.Search: {query}"
        )
    # [END add_customer_match_user_list_4]


def print_customer_match_user_list_info(
    client, customer_id, user_list_resource_name
):

    # [START add_customer_match_user_list_5]
    googleads_service_client = client.get_service("GoogleAdsService")

    # Creates a query that retrieves the user list.
    query = f"""
        SELECT
          user_list.size_for_display,
          user_list.size_for_search
        FROM user_list
        WHERE user_list.resource_name = '{user_list_resource_name}'"""

    # Issues a search request.
    search_results = googleads_service_client.search(
        customer_id=customer_id, query=query
    )
    # [END add_customer_match_user_list_5]

    # Prints out some information about the user list.
    user_list = next(iter(search_results)).user_list
    print(
        "The estimated number of users that the user list "
        f"'{user_list.resource_name}' has is "
        f"{user_list.size_for_display} for Display and "
        f"{user_list.size_for_search} for Search."
    )
    print(
        "Reminder: It may take several hours for the user list to be "
        "populated. Estimates of size zero are possible."
    )


def normalize_and_hash(s, remove_all_whitespace):
    # Normalizes by first converting all characters to lowercase, then trimming
    # spaces.
    if remove_all_whitespace:
        # Removes leading, trailing, and intermediate whitespace.
        s = "".join(s.split())
    else:
        # Removes only leading and trailing spaces.
        s = s.strip().lower()

    # Hashes the normalized string using the hashing algorithm.
    return hashlib.sha256(s.encode()).hexdigest()