import os
import subprocess


def convert_notebooks_to_html(directory):
    """
    Convert all Jupyter notebook files in the given directory to HTML using command line.
    """
    # Ensure the provided directory exists
    if not os.path.isdir(directory):
        raise ValueError(f"The provided directory '{directory}' does not exist.")

    # Loops through all jupyter notebook files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".ipynb"):
            # Construct the full file path
            file_path = os.path.join(directory, filename)

            # Use nbconvert command line tool to convert the notebook to HTML
            subprocess.run(
                ["jupyter", "nbconvert", file_path, "--to", "html_embed", "--no-input"],
                check=True,
            )

            print(f"Converted {filename} to HTML")


if __name__ == "__main__":
    notebooks_dir = "notebooks"
    convert_notebooks_to_html(notebooks_dir)
