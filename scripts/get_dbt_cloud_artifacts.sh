set -euo

RESPONSE=$(curl -H "Authorization: Token $DBT_TOKEN_VALUE" -H "Content-Type:application/json" -d '{"cause":"Triggered by GitHub Actions"}' https://cloud.getdbt.com/api/v2/accounts/$DBT_ACCOUNT_ID/jobs/$DBT_CLOUD_JOB_ID/run/);
STATUSCODE=$(echo "$RESPONSE" | jq '.status.code');

echo "Starting dbt cloud job:";
if [ $STATUSCODE != 200 ];
    then echo "$RESPONSE" && bash -c "exit 1";
fi;

RUN_ID=$(echo $RESPONSE | jq '.data.id');
echo "Run_id: $RUN_ID";
while true;
do
    sleep 5;
    WAITING=$(curl -s -G -H "Authorization:Token $DBT_TOKEN_VALUE" -H "Content-Type:application/json" "https://cloud.getdbt.com/api/v2/accounts/$DBT_ACCOUNT_ID/runs/$RUN_ID/");
    echo "Job status: $(echo $WAITING | jq '.data.status_humanized')";
    if ( $(echo $WAITING | jq '.data.is_complete') );
    then
        break;
    fi;
done;

mkdir -p ./tmp
curl -H "Authorization: Token $DBT_TOKEN_VALUE" https://cloud.getdbt.com/api/v2/accounts/$DBT_ACCOUNT_ID/runs/$RUN_ID/artifacts/catalog.json > './dbt_project/target/catalog.json'
curl -H "Authorization: Token $DBT_TOKEN_VALUE" https://cloud.getdbt.com/api/v2/accounts/$DBT_ACCOUNT_ID/runs/$RUN_ID/artifacts/manifest.json > './dbt_project/target/manifest.json'
curl -H "Authorization: Token $DBT_TOKEN_VALUE" https://cloud.getdbt.com/api/v2/accounts/$DBT_ACCOUNT_ID/runs/$RUN_ID/artifacts/run_results.json > './dbt_project/target/run_results.json'
