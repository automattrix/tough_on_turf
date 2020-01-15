from matplotlib import cm
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator, MaxNLocator)
import matplotlib.pyplot as plt
import seaborn as sns
import analysis


def graph_norm_velocity(play_df, name):
    sns.set(style='whitegrid')
    fig, ax = plt.subplots()

    cmap_red = cm.get_cmap('Reds')  # Colour map (there are many others)
    cmap_blue = cm.get_cmap('Blues')  # Colour map (there are many others)
    cmap_grey = cm.get_cmap('Greys')  # Colour map (there are many others)

    start_pt, end_pt = analysis.get_x_y_start_end(play_df[0])
    final_play_dfs = [df for df in play_df if not df.empty]
    for df in final_play_dfs:
        veltype = df['veltype'].iloc[0]
        if veltype == 'Positive':
            cmap = cmap_red
        elif veltype == 'Negative':
            cmap = cmap_blue
        else:
            cmap = cmap_grey

        df.plot(kind='scatter', x='x', y='y', c='norm_velocity_change', figsize=(22.5, 10), xlim=(0, 120), ylim=(0, 53.3), cmap=cmap, ax=ax, colorbar=False)
    #plt.imshow(play_df[0]['norm_velocity_change'], cmap=cmap_grey)

    ax.set(ylim=(0, 53.3))
    ax.set(xlim=(0, 120))
    ax.xaxis.set_major_locator(MaxNLocator(14))
    ax.xaxis.set_minor_locator(MultipleLocator(1))

    ax.set_xticklabels([0, 0, 10, 20, 30, 40, 50, 40, 30, 20, 10, 0, 0])
    plt.text(start_pt['x'], start_pt['y'], 'Start')
    plt.text(end_pt['x'], end_pt['y'], 'End')
    plt.savefig('./images/norm_velocity/{}'.format(name))
    plt.clf()
    plt.close()

def graph_route_arrows(play_df, name):
    start_pt = play_df[['x','y']].iloc[0]
    end_pt = play_df[['x', 'y']].iloc[-1]

    #ax = plt.axes()
    sns.set(style='whitegrid')
    cmap = cm.get_cmap('cool')


    ax = play_df.plot(kind='scatter', x='x', y='y', figsize=(22.5, 10), xlim=(0, 120), ylim=(0, 53.3), c='velocity',
                      cmap=cmap, colorbar=False, alpha=0)

    #ax.arrow(play_df['x'], play_df['x_comp'], play_df['y'], play_df['y_comp'], head_width=1, head_length=1, gc='k', ec='k')
    for index, row in play_df.iterrows():
        ax.arrow(row['x'], row['y'], row['x_comp'], row['y_comp'], head_width=0.5, head_length=0.5, fc='k', ec='k', alpha=0.5)
        ax.arrow(row['x'], row['y'], row['x_comp_dir'], row['y_comp_dir'], head_width=0.5, head_length=0.5, fc='b', ec='b',
                 alpha=0.5)
        #ax.arrow(row['x'], row['xy_comp'][0], row['y'], row['xy_comp'][1], head_width=1, head_length=1)

    #ax.xaxis.set_major_locator(MaxNLocator(14))
    #ax.xaxis.set_minor_locator(MultipleLocator(1))

    #ax.set_xticklabels([0, 0, 10, 20, 30, 40, 50, 40, 30, 20, 10, 0, 0])
    plt.text(start_pt['x'], start_pt['y'], 'Start')
    plt.text(end_pt['x'], end_pt['y'], 'End')
    plt.savefig('./images/vectors/{}'.format(name))
    plt.close()


def graph_route(play_df, name):
    start_pt = play_df[['x','y']].iloc[0]
    end_pt = play_df[['x', 'y']].iloc[-1]

    sns.set(style='whitegrid')
    cmap = cm.get_cmap('cool')  # Colour map (there are many others)

    ax = play_df.plot(kind='scatter', x='x', y='y', figsize=(22.5, 10), xlim=(0, 120), ylim=(0, 53.3), c='velocity',
                        cmap=cmap, colorbar=True)
    ax.xaxis.set_major_locator(MaxNLocator(14))
    ax.xaxis.set_minor_locator(MultipleLocator(1))

    ax.set_xticklabels([0, 0, 10, 20, 30, 40, 50, 40, 30, 20, 10, 0, 0])
    plt.text(start_pt['x'], start_pt['y'], 'Start')
    plt.text(end_pt['x'], end_pt['y'], 'End')
    plt.savefig('./images/velocity/{}'.format(name))
    plt.close()
