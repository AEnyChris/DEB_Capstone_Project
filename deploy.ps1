echo "### getting cluster credentials to initiate kubectl"
gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw location)
echo "### done"

echo "### creating namespace nfs and the nfs-server.yaml file apply"
kubectl create namespace nfs # creates namespace
kubectl -n nfs apply -f nfs/nfs-server.yaml # creates server
$env:NFS_SERVER=$(kubectl -n nfs get service/nfs-server -o jsonpath="{.spec.clusterIP}")
echo "--done"

echo "-### reating namespace storage"
kubectl create namespace storage

echo "### using helm to add nfs-subdir-external-provisioner repo"
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
echo "### done"

echo "### intalling nfs-subdir-external-provisioner"
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner --namespace storage --set nfs.server=$($env:NFS_SERVER) --set nfs.path=/

echo "### creating namespace airflow"
kubectl create namespace airflow

echo "### installing airflow commence"
helm repo add apache-airflow https://airflow.apache.org

helm upgrade --install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow
echo "### done"
kubectl get pods -n airflow

echo "####port forwarding for airflow webserver##"
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
