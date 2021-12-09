from diagrams import Cluster, Diagram
from diagrams.gcp.storage import GCS
from diagrams.gcp.analytics import Dataproc
from diagrams.gcp.analytics import Composer
from diagrams.onprem.analytics import Spark
from diagrams.generic.place import Datacenter
from diagrams.generic.storage import Storage
from diagrams.gcp.analytics import Bigquery
from diagrams.onprem.analytics import Tableau

with Diagram("Resonance Diagram", filename='./resonance-diagram', show=False):



    bi_tool = Tableau('BI Tool')
    with Cluster('Sources'):
        api = Datacenter('Music Apps API Batch')
        socials = Storage('Artist Social Media')
        events = Storage('Artist Events')
    with Cluster('Batch Process'):
        composer = Composer('airflow')
        dwh = Bigquery('Artist Stats')

        cluster = Dataproc('resonance-cluster')

        # with Cluster("Data Lake"):
        raw_bucket = GCS('Raw Layer')
        stg_bucket = GCS('Staging Layer')

    api >> raw_bucket
    socials >> raw_bucket
    events >> raw_bucket
    raw_bucket >> cluster >> stg_bucket >> dwh
    dwh >> bi_tool
