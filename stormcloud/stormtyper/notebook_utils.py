import ipywidgets as widgets
from IPython.display import display
import pandas as pd

# List of storm types
storm_types = ["mid_latitude_cyclone", "mesoscale_storm", "tropical", "local_storm"]


def init_stats_dict():
    """
    Build the summary stats dictionary used to track scoring for each storm
    """
    # Initialize summary_stats dictionary
    summary_stats = {
        "PREC_ACC_NC": {},
        "SBCAPE": {},
        "SLP": {},
        "PWAT": {},
        "SRH": {},
        "Z50000": {},
    }

    # Common structure for each parameter
    default_structure = {
        "mid_latitude_cyclone": 0,
        "mesoscale_storm": 0,
        "tropical": 0,
        "local_storm": 0,
        "notes": None,
    }

    # Apply the common structure to each parameter in summary_stats
    for param in summary_stats:
        summary_stats[param] = default_structure.copy()

    return summary_stats


def create_select_widget(parameter, summary_stats):
    """
    Function to create and display a SelectMultiple widget for a given parameter
    """
    select_widget = widgets.SelectMultiple(
        options=storm_types,
        value=[
            storm_type
            for storm_type in storm_types
            if summary_stats[parameter][storm_type] == 1
        ],
        description=f"{parameter} Storm Type:",
        disabled=False,
    )

    def update_dict(change):
        for storm_type in storm_types:
            summary_stats[parameter][storm_type] = (
                1 if storm_type in change["new"] else 0
            )

    select_widget.observe(update_dict, names="value")
    display(select_widget)


def create_notes_widget(parameter, summary_stats):
    """
    Function to create notes widgit for each parameter
    """
    notes_widget = widgets.Textarea(
        value=summary_stats[parameter]["notes"]
        if summary_stats[parameter]["notes"] is not None
        else "",
        description="Notes:",
        disabled=False,
    )

    def update_notes(change):
        summary_stats[parameter]["notes"] = change["new"]

    notes_widget.observe(update_notes, names="value")
    display(notes_widget)


def storm_type_analysis(df: pd.DataFrame):
    """
    Function to analyze storm type scores.
    """
    # Removing the notes row
    df = df.iloc[:-1]
    # Check if each parameter has values entered
    var_sums = df.sum(axis=0)
    for var, sum in var_sums.items():
        if sum == 0:
            print(f"No values were entered for {var}.\n")

    # Print total scores for each storm type
    storm_type_sums = df.sum(axis=1)
    print("Total scores by category:\n")
    for storm_type, score in storm_type_sums.items():
        if storm_type != "notes":
            print(f"{storm_type}: {score}")

    max_score = storm_type_sums.max()
    top_storm_types = storm_type_sums[storm_type_sums == max_score]
    # Print most likely storm type based on total scores
    if len(top_storm_types) > 1:
        print("A hybrid storm type is most likely.")
    else:
        print(f"The most likely storm type is {top_storm_types.idxmax()}.")
