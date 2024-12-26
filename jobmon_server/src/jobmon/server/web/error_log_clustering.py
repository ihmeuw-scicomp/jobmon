import nltk
from pandas import DataFrame
import sklearn.feature_extraction.text as ext

nltk.download("punkt", quiet=True)


def cluster_error_logs(df: DataFrame) -> DataFrame:
    """Cluster error logs using unsupervised learning."""
    count_vectorizer = ext.CountVectorizer()
    doc_matrix = count_vectorizer.fit_transform(df["task_instance_stderr_log"])

    # TF-IDF transformation
    tf_idf_transformer = ext.TfidfTransformer()
    log_scores = tf_idf_transformer.fit_transform(doc_matrix).toarray()

    per_log_score = log_scores.sum(axis=1) / (log_scores != 0).sum(axis=1)

    df["error_score"] = per_log_score

    df_grouped = (
        df.groupby("error_score")
        .agg(
            group_instance_count=("error_score", "count"),
            task_instance_ids=("task_instance_id", lambda x: list(set(x))),
            task_ids=("task_id", lambda x: list(set(x))),
            sample_error=("task_instance_stderr_log", "first"),
            first_error_time=("error_time", "first"),
            workflow_run_id=("workflow_run_id", "first"),
            workflow_id=("workflow_id", "first"),
        )
        .reset_index()
    )

    df_grouped.sort_values(by="group_instance_count", ascending=False, inplace=True)

    return df_grouped
