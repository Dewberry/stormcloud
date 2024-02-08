import ipywidgets as widgets
from IPython.display import display
import pandas as pd
import os
import scrapbook as sb
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

    # Update summary_stats dict based on widget input
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
        value=(
            summary_stats[parameter]["notes"]
            if summary_stats[parameter]["notes"] is not None
            else ""
        ),
        description="Notes:",
        disabled=False,
    )

    # Update summary_stats dict based on notes widget input
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
        print(f"The most likely storm type is {top_storm_types.index[0]}.")


def get_notebook_paths(notebooks_folder_path):
    """Gets path for every jupyter notebook within given folder."""
    notebook_paths = []
    for file_name in os.listdir(notebooks_folder_path):
        if file_name.endswith(".ipynb"):
            file_path = notebooks_folder_path + "/" + file_name
            notebook_paths.append(file_path)
    return notebook_paths


def determine_storm_type(row):
    """Takes dataframe column name with the highest score as the storm type"""
    max_value = row.max()
    max_columns = row[row == max_value].index.tolist()
    if len(max_columns) > 1:
        return "hybrid"
    else:
        return max_columns[0]


def process_notebooks(notebook_paths):
    """Loops through jupyter notebooks and uses scrapbook to gather stats from each one, converting the stats to a dataframe"""

    all_data_df = pd.DataFrame()
    for path in notebook_paths:
        storm_date = path.split("/")[-1].split(".")[0].split("-")[-1]
        nb = sb.read_notebook(path)
        scraps = nb.scraps
        stats_df = pd.DataFrame(scraps["summary_stats"].data)

        # Extract 'notes' row and adds them, then drop the row
        if "notes" in stats_df.index:
            notes_row = stats_df.loc["notes"]
            notes = " ".join(notes_row.dropna().astype(str))
            stats_df = stats_df.drop("notes")
        else:
            notes = ""

        storm_type_sums = stats_df.sum(axis=1)
        df = storm_type_sums.to_frame().T
        df.insert(0, "storm_start_date", storm_date)
        df["likely_storm_type"] = df.drop("storm_start_date", axis=1).apply(
            determine_storm_type, axis=1
        )

        # Add notes back to df
        df["notes"] = notes

        all_data = pd.concat([all_data, df], ignore_index=True)
    return all_data_df


def convert_paths_to_html_links(data_frame, notebook_paths):
    """Add HTML hyperlinks to dataframe"""

    html_paths = [path.replace(".ipynb", ".html") for path in notebook_paths]
    data_frame["HTML_Link"] = html_paths
    data_frame["HTML_Link"] = data_frame["HTML_Link"].apply(
        lambda x: f"<a href='{x}'>{x}</a>"
    )
    return data_frame
