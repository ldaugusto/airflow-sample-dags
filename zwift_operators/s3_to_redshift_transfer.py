from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftTransfer(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_file: reference to a specific S3 object (bucket and key)
        or objects prefix for parallel Redshift load
    :type s3_file: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_file,
            redshift_conn_id='redshift_default',
            verify=None,
            copy_options=tuple(),
            autocommit=False,
            *args, **kwargs):
        super(S3ToRedshiftTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_file = s3_file
        self.redshift_conn_id = redshift_conn_id
        self.verify = verify
        self.copy_options = copy_options
        self.autocommit = autocommit

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print(self.hook.get_conn().__dict__.items())
        redshift_extras = self.hook.get_conn().extra_dejson
        self.iam_role = redshift_extras.get('iam_role')
        if self.iam_role is None:
            raise AirflowException('No iam_role file provided in extras')

    def execute(self, context):
        copy_options = '\n\t\t\t'.join(self.copy_options)

        copy_query = """
            COPY {schema}.{table}
            FROM '{file}'
            iam_role {iam_role}
            {copy_options};
        """.format(schema=self.schema,
                   table=self.table,
                   file=self.s3_file,
                   iam_role=self.iam_role,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
