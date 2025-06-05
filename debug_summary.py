import json
import numpy as np
from api.endpoints.month_wise_analysis import generate_monthly_summary

def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(i) for i in obj]
    elif isinstance(obj, np.generic):
        return obj.item()
    return obj

if __name__ == "__main__":
    username = "ayush"
    print(f"🔍 Generating monthly summary for user: {username}\n")

    summaries = generate_monthly_summary(username)

    if summaries is None:
        print("❌ No summary generated. User not found or no transactions available.")
    else:
        summaries_cleaned = convert_numpy_types(summaries)

        for i, summary in enumerate(summaries_cleaned):
            print(f"\n📅 Summary for Month #{i + 1}:")
            for k, v in summary.items():
                print(f"{k:<30}: {v} ({type(v)})")

        # Optional: Print as pretty JSON
        print("\n📦 Final Output as JSON:")
        print(json.dumps(summaries_cleaned, indent=2))
