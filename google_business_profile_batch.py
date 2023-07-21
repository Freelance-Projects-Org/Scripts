from googleapiclient.discovery import build, BatchHttpRequest
from google_auth_oauthlib.flow import InstalledAppFlow

client_file = 'client_secret.json'
scopes = ['https://www.googleapis.com/auth/business.manage']
flow = InstalledAppFlow.from_client_secrets_file(client_file, scopes)
creds = flow.run_local_server(port=0)
metrics_result = []

def get_locations_for_account(account:str, readMask:str):
    #get all locations for respective account using Business Information API 
    business_info_service = build('mybusinessbusinessinformation', 'v1', credentials=creds)
    get_locations_resp = business_info_service.accounts().locations().list(parent=account, readMask=readMask).execute()["locations"]
    while 'nextPageToken' in get_locations_resp.keys():
        resp = business_info_service.accounts().locations().list(parent=account, readMask=readMask, nextPageToken=get_locations_resp["nextPageToken"]).execute()["locations"]
        get_locations_resp.extend(resp)

    locations = [i["name"] for i in get_locations_resp]
    # client_business_info.close() #closes httplib2 connections. Uncomment if you want to close the client connection
    return locations

def process_metrics_response(request_id, response, exception):
    if exception is not None:
        raise
    else:
        metrics_result.append(response["multiDailyMetricTimeSeries"])

#get metrics for all locations for respective account using Business Profile Performance API 
def get_metrics(locations:list, batch:BatchHttpRequest):
    business_profile_service = build('businessprofileperformance', 'v1', credentials=creds)
    daily_metrics = ['WEBSITE_CLICKS']
    for location in locations:
        batch.add(business_profile_service.locations().fetchMultiDailyMetricsTimeSeries(
            location = location,
            dailyMetrics = daily_metrics,
            dailyRange_endDate_day=2, 
            dailyRange_endDate_month=5, 
            dailyRange_endDate_year=2023, 
            dailyRange_startDate_day=1, 
            dailyRange_startDate_month=5, 
            dailyRange_startDate_year=2023))
    batch.execute()

def main():
    readMask = "name"
    account = "accounts/110434597297568841447" #replace with your account id. Make sure to follow the pattern 'accounts/<your_id>'. Refer https://developers.google.com/my-business/reference/accountmanagement/rest/v1/accounts/list to get all accounts
    locations = get_locations_for_account(account=account, readMask=readMask)
    batch = BatchHttpRequest(callback=process_metrics_response)
    get_metrics(locations=locations, batch=batch)
    return metrics_result

if __name__ == '__main__':
    main()
