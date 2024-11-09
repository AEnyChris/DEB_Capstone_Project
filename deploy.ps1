nslookup myip.opendns.com resolver1.opendns.com

Write-Output "### getting cluster credentials to initiate kubectl"
gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw location)
Write-Output "### done"

Write-Output "### creating namespace nfs and the nfs-server.yaml file apply"
kubectl create namespace nfs # creates namespace
kubectl -n nfs apply -f nfs/nfs-server.yaml # creates server
$env:NFS_SERVER=$(kubectl -n nfs get service/nfs-server -o jsonpath="{.spec.clusterIP}")
Write-Output "--done"

Write-Output "-### creating namespace storage"
kubectl create namespace storage

Write-Output "### using helm to add nfs-subdir-external-provisioner repo"
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
Write-Output "### done"

Write-Output "### intalling nfs-subdir-external-provisioner"
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner --namespace storage --set nfs.server=$($env:NFS_SERVER) --set nfs.path=/

Write-Output "### creating namespace airflow"
kubectl create namespace airflow

Write-Output "### installing airflow commence"
helm repo add apache-airflow https://airflow.apache.org

helm upgrade --install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow
Write-Output "### done"
kubectl get pods -n airflow

Write-Output "####port forwarding for airflow webserver##"
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
