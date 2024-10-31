import seaborn as sns
from textwrap import fill

import numpy as np

from sklearn.metrics import (
    classification_report,
    recall_score,
    precision_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
)
import matplotlib.pyplot as plt
from sklearn.metrics import (
    roc_curve,
    auc,
    precision_recall_curve,
    confusion_matrix,
)


def draw_roc_curve(ax, fpr, tpr, roc_auc):
    ax.plot(
        fpr, tpr, color="darkorange", lw=2, label="ROC curve (area = %0.2f)" % roc_auc
    )
    ax.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title("Receiver Operating Characteristic")
    ax.legend(loc="lower right")


def draw_pr_curve(ax, precision, recall, pr_auc):
    ax.plot(
        recall,
        precision,
        color="darkblue",
        lw=2,
        label="PR curve (area = %0.2f)" % pr_auc,
    )
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall")
    ax.legend(loc="lower right")


def draw_score_dist(ax, y_proba, y_true):
    ax.hist(y_proba[y_true == 0], bins=50, alpha=0.5, label="Negative class")
    ax.hist(y_proba[y_true == 1], bins=50, alpha=0.5, label="Positive class")
    ax.set_xlabel("Score")
    ax.set_ylabel("Frequency")
    ax.set_title("Score Distribution")
    ax.legend(loc="upper right")


def draw_confusion_matrix(ax, conf_matrix, fmt="d", title="Confusion Matrix"):
    sns.heatmap(conf_matrix, annot=True, ax=ax, cmap="Blues", fmt=fmt)
    ax.set_xlabel("Predicted labels")
    ax.set_ylabel("True labels")
    ax.set_title(title)
    ax.xaxis.set_ticklabels(["Negative", "Positive"])
    ax.yaxis.set_ticklabels(["Negative", "Positive"])


def draw_calibration_curve(ax, y_true, y_proba):
    from sklearn.calibration import calibration_curve

    fraction_of_positives, mean_predicted_value = calibration_curve(
        y_true, y_proba, n_bins=10
    )

    ax.plot(
        mean_predicted_value, fraction_of_positives, "s-", label="Calibration curve"
    )
    ax.plot([0, 1], [0, 1], "--", color="gray", label="Perfectly calibrated")
    ax.set_xlabel("Mean predicted value")
    ax.set_ylabel("Fraction of positives")
    ax.set_title("Calibration Curve")
    ax.legend(loc="upper left")


def visual_report(y_true, y_pred, y_proba, title="Model Performance", is_simple=False):
    # Compute ROC curve and ROC area
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    roc_auc = auc(fpr, tpr)

    # Compute Precision-Recall curve and AP
    precision, recall, _ = precision_recall_curve(y_true, y_proba)
    ap = average_precision_score(y_true, y_proba)

    # Generate confusion matrix
    conf_matrix = confusion_matrix(y_true, y_pred)
    conf_matrix_norm = confusion_matrix(y_true, y_pred, normalize="pred")

    if not is_simple:
        # Plotting
        fig, ax = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle(title)

        # ROC Curve
        draw_roc_curve(ax[0, 0], fpr, tpr, roc_auc)

        # Precision-Recall Curve
        draw_pr_curve(ax[0, 1], precision, recall, ap)

        # Score Distribution
        draw_score_dist(ax[0, 2], y_proba, y_true)

        # Calibration Curve
        draw_calibration_curve(ax[1, 0], y_true, y_proba)

        # Confusion Matrix
        draw_confusion_matrix(ax[1, 1], conf_matrix)

        # Confusion Matrix (Normalized)
        draw_confusion_matrix(
            ax[1, 2], conf_matrix_norm, fmt=".2f", title="Confusion Matrix (pred)"
        )
    else:
        fig, ax = plt.subplots(1, 2, figsize=(10, 5))
        fig.suptitle(title)

        # Precision-Recall Curve
        draw_pr_curve(ax[0], precision, recall, ap)

        # Confusion Matrix
        draw_confusion_matrix(ax[1], conf_matrix)

    plt.tight_layout()

    return fig


def visual_hard_report(y_true, y_pred, title="Model Hard Performance"):
    # Generate confusion matrix
    conf_matrix = confusion_matrix(y_true, y_pred)
    conf_matrix_norm = confusion_matrix(y_true, y_pred, normalize="pred")

    # Plotting
    fig, ax = plt.subplots(1, 2, figsize=(10, 5))
    fig.suptitle(title)

    # Confusion Matrix
    draw_confusion_matrix(ax[0], conf_matrix)

    # Confusion Matrix (Normalized)
    draw_confusion_matrix(
        ax[1], conf_matrix_norm, fmt=".2f", title="Confusion Matrix (pred)"
    )

    plt.tight_layout()

    return fig


def numeric_report(y_true, y_pred, y_proba):
    print(classification_report(y_true, y_pred))

    hard_recall = recall_score(y_true, y_pred)
    hard_precision = precision_score(y_true, y_pred)
    hard_f1 = f1_score(y_true, y_pred)

    auc_roc = roc_auc_score(y_true, y_proba)
    auc_pr = average_precision_score(y_true, y_proba)

    print(f"Recall: {hard_recall:.2f}")
    print(f"Precision: {hard_precision:.2f}")
    print(f"F1: {hard_f1:.2f}")
    print(f"AUC ROC: {auc_roc:.2f}")
    print(f"AUC PR: {auc_pr:.2f}")

    positive_class_ratio = np.mean(y_true)
    random_pred = np.random.binomial(1, positive_class_ratio, size=len(y_true))

    hard_f1_baseline = f1_score(y_true, random_pred)

    print(f"F1 Lift (random): {(hard_f1 / hard_f1_baseline):.2f}")
    print(f"F1 score baseline (random): {hard_f1_baseline:.2f}")

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba, pos_label=1)
    precision = np.array(precision[1:])
    recall = np.array(recall[1:])

    fixed_recall = 0.95
    precision_at_fixed_recall = precision[recall >= fixed_recall][-1]
    threshold_at_fixed_recall = thresholds[recall >= fixed_recall][-1]

    print(f"Precision at {fixed_recall:.2f} recall: {precision_at_fixed_recall:.2f}")
    print(f"Threshold at {fixed_recall:.2f} recall: {threshold_at_fixed_recall:.2f}")

    return {
        "recall": hard_recall,
        "precision": hard_precision,
        "f1": hard_f1,
        "auc_roc": auc_roc,
        "auc_pr": auc_pr,
        "f1_lift": hard_f1 / hard_f1_baseline,
        "f1_baseline": hard_f1_baseline,
    }


def numeric_hard_report(y_true, y_pred):
    print(classification_report(y_true, y_pred))

    hard_recall = recall_score(y_true, y_pred)
    hard_precision = precision_score(y_true, y_pred)
    hard_f1 = f1_score(y_true, y_pred)

    print(f"Recall: {hard_recall:.2f}")
    print(f"Precision: {hard_precision:.2f}")
    print(f"F1: {hard_f1:.2f}")

    positive_class_ratio = np.mean(y_true)
    random_pred = np.random.binomial(1, positive_class_ratio, size=len(y_true))

    hard_f1_baseline = f1_score(y_true, random_pred)

    print(f"F1 Lift (random): {(hard_f1 / hard_f1_baseline):.2f}")
    print(f"F1 score baseline (random): {hard_f1_baseline:.2f}")

    return {
        "recall": hard_recall,
        "precision": hard_precision,
        "f1": hard_f1,
        "f1_lift": hard_f1 / hard_f1_baseline,
        "f1_baseline": hard_f1_baseline,
    }


def print_analysis(test_ds, top_n, ascending):
    for i, row in (
        test_ds.sort_values("y_proba_diff", ascending=ascending).head(top_n).iterrows()
    ):
        print(
            f"> query_result_hash_id = {row['query_result_hash_id']} | title_text_hash_id = {row['title_text_hash_id']}"
        )
        print(row["page_url"])
        print(row["status"])
        print(
            f"> y_pred = {row['y_pred']} | y_true = {row['y_true']} | y_pred_proba = {row['y_pred_proba']:.2f} | y_proba_diff = {row['y_proba_diff']:.2f}"
        )
        print(f"> Query: {row['keyword_query']}")
        print(f"> Title: {fill(row['title'], width=100)}")
        print("> Text:")
        print(fill(row["text"][:1500], width=100))
        print("\n" + "-" * 100 + "\n")
