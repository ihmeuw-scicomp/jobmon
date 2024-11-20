import nltk
from nltk import corpus, word_tokenize
from nltk.stem.porter import PorterStemmer
from pandas import DataFrame
import sklearn.feature_extraction.text as ext
from typing import List

# Download necessary NLTK resources once
nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)

# Global stopwords list
stopwords = corpus.stopwords.words("english")
stopwords.extend(["could", "might", "must", "need", "sha", "wo", "would"])


def cluster_error_logs(df: DataFrame) -> DataFrame:
    """Cluster error logs using unsupervised learning."""

    def log_tokenizer(text: str) -> List[str]:
        # Tokenize by sentence, then by word, and apply stemming
        tokens = word_tokenize(text.lower())
        tokens = [word for word in tokens if word.isalpha() and word not in stopwords]
        stemmer = PorterStemmer()
        return [stemmer.stem(token) for token in tokens]

    # Learn the vocabulary dictionary and return document-term matrix.
    count_vectorizer = ext.CountVectorizer(tokenizer=log_tokenizer, stop_words=stopwords, token_pattern=None)
    doc_matrix = count_vectorizer.fit_transform(df["error"])

    # Learn the idf vector (global term weights), then Tf-idf-weighted document-term matrix
    tf_idf_transformer = ext.TfidfTransformer()
    log_scores = tf_idf_transformer.fit_transform(doc_matrix).toarray()

    # Calculate error scores
    per_log_score = log_scores.sum(axis=1) / (log_scores != 0).sum(axis=1)  # Avoid division by zero

    df["error_score"] = per_log_score

    # Group by error score and aggregate
    df_grouped = df.groupby("error_score").agg(
        group_instance_count=("error_score", "count"),
        task_instance_ids=("task_instance_id", lambda x: list(set(x))),
        task_ids=("task_id", lambda x: list(set(x))),
        sample_error=("error", "first"),
        first_error_time=("error_time", "first"),
        workflow_run_id=("workflow_run_id", "first"),
        workflow_id=("workflow_id", "first")
    ).reset_index()

    # Sort by group instance count
    df_grouped.sort_values(by="group_instance_count", ascending=False, inplace=True)

    return df_grouped
