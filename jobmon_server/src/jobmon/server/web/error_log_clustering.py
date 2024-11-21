import nltk
from nltk import corpus, word_tokenize
from nltk.stem.porter import PorterStemmer
from pandas import DataFrame
import sklearn.feature_extraction.text as ext
from typing import List
import time

nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)

stopwords = corpus.stopwords.words("english")
stopwords.extend(["could", "might", "must", "need", "sha", "wo", "would"])


def cluster_error_logs(df: DataFrame) -> DataFrame:
    """Cluster error logs using unsupervised learning."""
    start_time = time.time()

    def log_tokenizer(text: str) -> List[str]:
        # Tokenize by sentence, then by word, and apply stemming
        tokens = word_tokenize(text.lower())
        tokens = [word for word in tokens if word.isalpha() and word not in stopwords]
        stemmer = PorterStemmer()
        return [stemmer.stem(token) for token in tokens]

    count_vectorizer = ext.CountVectorizer(tokenizer=log_tokenizer, stop_words=stopwords, token_pattern=None)
    doc_matrix = count_vectorizer.fit_transform(df["error"])

    tf_idf_transformer = ext.TfidfTransformer()
    log_scores = tf_idf_transformer.fit_transform(doc_matrix).toarray()

    per_log_score = log_scores.sum(axis=1) / (log_scores != 0).sum(axis=1)  # Avoid division by zero

    df["error_score"] = per_log_score

    df_grouped = df.groupby("error_score").agg(
        group_instance_count=("error_score", "count"),
        task_instance_ids=("task_instance_id", lambda x: list(set(x))),
        task_ids=("task_id", lambda x: list(set(x))),
        sample_error=("error", "first"),
        first_error_time=("error_time", "first"),
        workflow_run_id=("workflow_run_id", "first"),
        workflow_id=("workflow_id", "first")
    ).reset_index()

    df_grouped.sort_values(by="group_instance_count", ascending=False, inplace=True)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.4f} seconds")

    return df_grouped
