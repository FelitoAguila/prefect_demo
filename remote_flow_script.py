from prefect import flow, task
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from prefect_github import GitHubCredentials
from prefect_gcp import GcpCredentials
import os

def set_github_pat_env():

    pat = GitHubCredentials.load("github-pat").token.get_secret_value()
    os.environ["SOURCES__ACCESS_TOKEN"] = pat 

def make_bq_destination():

    #get service account info
    gcp = GcpCredentials.load("gcp-creds")
    creds = gcp.service_account_info.get_secret_value() or {}
    #get project id
    project = creds.get("project_id")
    #create a bigquery destination
    return dlt.destinations.bigquery(credentials=creds, project_id=project)

@task(log_prints=True)
def run_resource(resource_name: str, bq_dest: dlt.destinations.bigquery):
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {
                # To run the pipeline locally
                "token": dlt.secrets["sources.github.access_token"],
            },
            "headers": {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            "paginator": "header_link",
        },
        "resources": [
            {
                "name": "repos",
                "endpoint": {"path": "orgs/dlt-hub/repos"},
            },
            {
                "name": "contributors",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/contributors",
                },
            },
            {
                "name": "issues",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues",
                    "params": {
                        "state": "open",  # Only get open issues
                        "sort": "updated",
                        "direction": "desc",
                        "since": "{incremental.start_value}",  # For incremental loading
                    },
                    "incremental": {
                        "cursor_path": "updated_at",
                        "initial_value": "2025-07-01T00:00:00Z",
                    },
                },
            },
            {
                "name": "forks",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/forks",
                    "params": {
                        "sort": "oldest",  # Ensures ascending creation order
                        "per_page": 100,
                    },
                    "incremental": {  # backfill
                        "cursor_path": "created_at",
                        "initial_value": "2025-07-01T00:00:00Z",
                        "row_order": "asc",
                    },
                },
            },
            {
                "name": "releases",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/releases",
                },
            },
        ],
    }

    github_source = rest_api_source(config).with_resources(resource_name)

    pipeline = dlt.pipeline(
        pipeline_name=f"github_remote_demo_{resource_name}",
        destination=bq_dest,
        dataset_name="demo_dynamic_github",
        progress="log"
    )

    info = pipeline.run(github_source)
    print(f"{resource_name} -> {info}")
    return info

@flow(log_prints=True)
def main():

    #set env variables
    set_github_pat_env()
    #create bigquery destination
    bq_dest = make_bq_destination()

    a = run_resource("repos", bq_dest)
    b = run_resource("contributors", bq_dest)
    c = run_resource("releases", bq_dest)
    return a, b, c

if __name__ == "__main__":
    # for a single local run
    # main()

    # for local deployment
    # main.serve(name="dynamic-deployment") 

    # cloud deployment
    main()