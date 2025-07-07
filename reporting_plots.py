import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.lines as mlines

from reporting_config import carrier_color_dict_reporting, dl_color_dict


# DL Network Category graph
def plot_dl_5g_network_category(df_dl_5g_curr, df_dl_5g_comp, dl_color_dict, save_path="graph.png"):

    df_dl_5g_curr["product_period"] = pd.Categorical(df_dl_5g_curr["product_period"], ordered=True)
    df_dl_5g_comp["product_period"] = pd.Categorical(df_dl_5g_comp["product_period"], ordered=True)
   
    df_dl_5g_combined = pd.concat([df_dl_5g_comp, df_dl_5g_curr], ignore_index=True)

    df_dl_5g_combined['normalized_pct'] = df_dl_5g_combined.groupby(['carrier', 'product_period'])['dl_pct'].transform(lambda x: x / x.sum() * 100)
    
    g = sns.FacetGrid(df_dl_5g_combined, col="carrier", col_wrap=4, height=7, aspect=1.2, sharey=True)

    g.map_dataframe(
        sns.barplot, 
        x="product_period", 
        y="normalized_pct", 
        hue="dl_network",
        palette=dl_color_dict, 
        dodge=False, 
        errorbar=None)

    for ax, (_, subdata) in zip(g.axes.flat, df_dl_5g_combined.groupby("carrier")):
        for container in ax.containers:
            for bar in container:
                height = bar.get_height()
                if height > 3: 
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.,
                        height / 2,
                        f"{int(height)}%",
                        ha="center",
                        va="center",
                        color="white",
                        fontsize=10
                    )

    g.set_axis_labels("", "Share of DL tests (%)", size=12)
    g.set_titles(col_template="{col_name}", size=14, fontweight="bold")
    g.set(ylim=(0, 100)) 

    g.add_legend(title="", loc="right", bbox_to_anchor=(0.5, 0.1), ncol=4, fontsize=12)
    g._legend.set_frame_on(False)

    g.fig.set_size_inches(20, 10)

    g.fig.subplots_adjust(top=0.85, bottom=0.2, left=0.1, right=0.9)

    g.savefig(save_path, dpi=300)
    plt.show()


def create_facet_grid(data, y_col, num_col_wrap, mean_curr_vals, mean_comp_vals, 
                      y_label, min_vals=None, max_vals=None, ylim=None, save_path="graph.png"):    
   
   g = sns.FacetGrid(
        data,
        col="carrier",
        col_wrap=num_col_wrap,
        height=4,
        sharey=True,
    )

   g.set_titles(col_template="{col_name}", size=14, fontweight='bold')

   g.map_dataframe(
        plot_with_summary_lines,
        y_col=y_col,
        mean_curr_vals=mean_curr_vals,
        mean_comp_vals=mean_comp_vals,
        min_vals=min_vals,
        max_vals=max_vals,
        color=None
    )

   max_value = data[y_col].max()

   if ylim is None:
        max_value = data[y_col].max()
        if max_value < 6:
            ylim = (0, max_value + 2)  
        else:
            ylim = (0, max_value + 20)  

   g.set_titles(col_template="{col_name}", size=14, fontweight="bold")
   g.set_axis_labels("", y_label)
   g.set(ylim=ylim)

   for ax in g.axes.flat:
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)  
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")  

   plt.subplots_adjust(top=0.9)

   mean_curr_line = mlines.Line2D([], [], color='blue', linewidth=2, label='Mean of current daily values')
   mean_comp_line = mlines.Line2D([], [], color='yellow', linewidth=2, label='Mean of previous daily values')
   legend_handles = [mean_curr_line, mean_comp_line]
    
   if min_vals is not None:
        min_comp_line = mlines.Line2D([], [], color='#686868', linewidth=1, linestyle='dashed', label='Min of previous daily values')
        legend_handles.append(min_comp_line)

   if max_vals is not None:
        max_comp_line = mlines.Line2D([], [], color='green', linewidth=1, linestyle='dashed', label='Max of previous daily values')
        legend_handles.append(max_comp_line)

   g.add_legend()  
   g.fig.legend(
        handles=legend_handles,
        loc='upper right',
        bbox_to_anchor=(0.99, 1.15),
        fontsize=8,
        frameon=True,
        edgecolor='black'
    )
   plt.tight_layout()

   g.savefig(save_path)
   plt.show()


def plot_with_summary_lines(data, y_col, mean_curr_vals, mean_comp_vals, color, min_vals=None, max_vals=None, **kwargs):
   
    carrier = data['carrier'].iloc[0]  

    sns.set_style("whitegrid")
    plt.grid(True, color='gray', linestyle='--', linewidth=0.5)

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