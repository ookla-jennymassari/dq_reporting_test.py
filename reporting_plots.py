import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import seaborn as sns

load_dotenv()

from reporting_config import *

# Define the create_facet_grid function
def create_facet_grid(data, y_col, num_col_wrap, mean_curr_vals, mean_comp_vals, 
                      y_label, title, min_vals=None, max_vals=None, ylim=None, save_path="graph.png"):    
   
   g = sns.FacetGrid(
        data,
        col="carrier",
        col_wrap=num_col_wrap,
        height=4,
        sharey=True
    )

    # Pass y_col and other arguments as keyword arguments
   g.map_dataframe(
        plot_with_fallback,
        y_col=y_col,
        mean_curr_vals=mean_curr_vals,
        mean_comp_vals=mean_comp_vals,
        min_vals=min_vals,
        max_vals=max_vals,
        color=None
    )

    # Calculate the maximum value for the y-axis
   max_value = data[y_col].max()

    # Calculate the default maximum value for the y-axis if ylim is not provided
   if ylim is None:
        max_value = data[y_col].max()
        if max_value < 6:
            ylim = (0, max_value + 2)  
        else:
            ylim = (0, max_value + 20)  

    # Customize the grid
   g.set_titles(col_template="{col_name}")
   g.set_axis_labels("", y_label)
   g.set(ylim=ylim)

# Adjust the x-axis tick labels (dates)
   for ax in g.axes.flat:
        ax.tick_params(axis='x', labelsize=8)  
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")  

   plt.subplots_adjust(top=0.9)
   g.fig.suptitle(title, fontsize=18, fontweight="bold")

    # Create custom legend handles
   import matplotlib.lines as mlines
   mean_curr_line = mlines.Line2D([], [], color='blue', linewidth=2, label='Mean of current daily values')
   mean_comp_line = mlines.Line2D([], [], color='yellow', linewidth=2, label='Mean of previous daily values')
   legend_handles = [mean_curr_line, mean_comp_line]
    
   if min_vals is not None:
        min_comp_line = mlines.Line2D([], [], color='#686868', linewidth=1, linestyle='dashed', label='Min of previous daily values')
        legend_handles.append(min_comp_line)

   if max_vals is not None:
        max_comp_line = mlines.Line2D([], [], color='green', linewidth=1, linestyle='dashed', label='Max of previous daily values')
        legend_handles.append(max_comp_line)

    # Add to the figure legend (not individual subplots)
   g.add_legend()  
   g.fig.legend(
        handles=legend_handles,
        loc='upper right',
        bbox_to_anchor=(0.99, 0.99),
        fontsize=6,
        frameon=True,
        edgecolor='black'
    )
   plt.tight_layout()
   g.savefig(save_path)
   plt.show()

# Define the plot_with_fallback function
def plot_with_fallback(data, y_col, mean_curr_vals, mean_comp_vals, color, min_vals=None, max_vals=None, **kwargs):
    import matplotlib.pyplot as plt

    # Get the carrier for the current facet
    carrier = data['carrier'].iloc[0]  

    sns.set_style("whitegrid")
    plt.grid(True, color='gray', linestyle='--', linewidth=0.5)

    # Plot the line for daily LTE percentage
    sns.lineplot(
        data=data,
        x="start_date",
        y=y_col,  
        marker="o",
        color=carrier_color_dict_reporting.get(carrier, "gray"),
        linewidth=3,
        markersize=10,
        **kwargs
    )

    # Add summary lines
    if carrier in mean_curr_vals:
        mean_curr = mean_curr_vals[carrier]
        plt.axhline(
            y=mean_curr,
            color="blue",
            linewidth=2,
            linestyle="-"
        )

    if carrier in mean_comp_vals:
        mean_comp = mean_comp_vals[carrier]
        plt.axhline(
            y=mean_comp,
            color="yellow",
            linewidth=2,
            linestyle="-"
        )

    if min_vals is not None and carrier in min_vals:
        min_val = min_vals[carrier]
        plt.axhline(
            y=min_val,
            color="#686868",
            linewidth=1,
            linestyle="dashed"
        )
    
    if max_vals is not None and carrier in max_vals:
        max_val = max_vals[carrier]
        plt.axhline(
            y=max_val,
            color="green",
            linewidth=1,
            linestyle="dashed"
        )