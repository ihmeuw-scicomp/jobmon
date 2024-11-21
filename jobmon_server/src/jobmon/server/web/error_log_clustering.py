import nltk
from nltk import corpus, word_tokenize
from pandas import DataFrame
import sklearn.feature_extraction.text as ext
import re

nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)

stopwords = corpus.stopwords.words("english")
stopwords.extend(["could", "might", "must", "need", "sha", "wo", "would"])


def cluster_error_logs(df: DataFrame) -> DataFrame:
    """Cluster error logs using unsupervised learning."""

    def preprocess_text(text):
        text = re.sub(r'[^\w\s]', '', text)
        text = text.lower()
        return text

    df["error"] = df["error"].apply(preprocess_text)
    count_vectorizer = ext.CountVectorizer()
    doc_matrix = count_vectorizer.fit_transform(df["error"])

    # TF-IDF transformation
    tf_idf_transformer = ext.TfidfTransformer()
    log_scores = tf_idf_transformer.fit_transform(doc_matrix).toarray()

    # Per-log score calculation
    per_log_score = log_scores.sum(axis=1) / (log_scores != 0).sum(axis=1)

    df["error_score"] = per_log_score

    df_grouped = df.groupby("error_score").agg(
        group_instance_count=("error_score", "count"),
        task_instance_ids=("task_instance_id", lambda x: list(set(x))),
        task_ids=("task_id", lambda x: list(set(x))),
        sample_error=("error", "first"),
        first_error_time=("error_time", "first"),
        workflow_run_id=("workflow_run_id", "first"),
        workflow_id=("workflow_id", "first"),
    ).reset_index()

    df_grouped.sort_values(by="group_instance_count", ascending=False, inplace=True)

    return df_grouped
