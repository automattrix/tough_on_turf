import seaborn as sns
import matplotlib.pyplot as plt

pal = ['steelblue', 'lightgreen']
dm_keys = ['DM_M1', 'DM_M7', 'DM_M28', 'DM_M42']
days_missed = ['1+ Days', '7+ Days', '28+ Days', '42+ Days']


def surface(injuryrecord):
    df_surface = injuryrecord.copy()

    df_injure_field = df_surface.groupby(
        ['Surface']).count()[['PlayerKey']].reset_index()
    print(df_injure_field)

    sns.set(style='whitegrid')
    f, axes = plt.subplots(figsize=(6, 8))
    sns.despine()

    f.suptitle("Total Injuries by Playing Surface", fontsize=24)

    sns.barplot(data=df_injure_field, x='Surface', y='PlayerKey', palette=pal, ax=axes)
    axes.set_xlabel("Playing Surface", fontsize=14)
    axes.set_ylabel("Number of Injuries", fontsize=14)

    plt.savefig('./data/08_reporting/surface.png')


def surface_length(injuryrecord):
    df_surface = injuryrecord.copy()
    df_injure_field = df_surface.groupby(
        ['Surface']).sum()[['DM_M1', 'DM_M7', 'DM_M28', 'DM_M42']].reset_index()
    print(df_injure_field)
    print(df_injure_field.keys())

    sns.set(style='whitegrid')
    f, axes = plt.subplots(figsize=(25, 10), ncols=4, sharey=True)
    sns.despine()

    f.suptitle("Instances of Days Missed by BodyPart and Playing Surface", fontsize=24)
    for x, key in enumerate(dm_keys):
        sns.barplot(data=df_injure_field, x='Surface', y=key, hue='Surface', palette=pal, ax=axes[x])
        axes[x].set_ylabel(days_missed[x], fontsize=14)
    # axes[0].set_ylabel("1+ Days ", fontsize=14)
    plt.savefig('./data/08_reporting/surface_length.png')


def surface_bodypart_length_w_null(injuryrecord):
    df_w_null = injuryrecord.copy()
    df_injure_field = df_w_null.groupby(
        ['BodyPart', 'Surface']).sum()[['DM_M1', 'DM_M7', 'DM_M28', 'DM_M42']].reset_index()
    print(df_injure_field)

    sns.set(style='whitegrid')
    f, axes = plt.subplots(figsize=(25, 10), ncols=4, sharey=True)
    sns.despine()

    f.suptitle("Instances of Days Missed by BodyPart and Playing Surface", fontsize=24)

    for x, key in enumerate(dm_keys):
        sns.barplot(data=df_injure_field, x='BodyPart', y=key, hue='Surface', palette=pal, ax=axes[x])
        axes[x].set_ylabel(days_missed[x], fontsize=14)
        if key is not dm_keys[-1]:
            axes[x].legend_.remove()

    plt.savefig('./data/08_reporting/surface_bodypart_length_w_null.png')


def surface_bodypart_length(df, injuryrecord):
    df_injure = injuryrecord.dropna()
    df_injure_field = df_injure.groupby(
        ['BodyPart', 'Surface']).sum()[['DM_M1', 'DM_M7', 'DM_M28', 'DM_M42']].reset_index()

    sns.set(style='whitegrid')
    f, axes = plt.subplots(figsize=(25, 10), ncols=4, sharey=True)
    sns.despine()

    f.suptitle("Instances of Days Missed by BodyPart and Playing Surface", fontsize=24)

    for x, key in enumerate(dm_keys):
        sns.barplot(data=df_injure_field, x='BodyPart', y=key, hue='Surface', palette=pal, ax=axes[x])
        axes[x].set_ylabel(days_missed[x], fontsize=14)
        if key is not dm_keys[-1]:
            axes[x].legend_.remove()

    plt.savefig('./data/08_reporting/surface_bodypart_length.png')


def graph_injury(df):
    # print(df.head())
    # keys = (df['playkey'])
    # print(keys)

    # Testing
    # test = games_df.loc[games_df['game_key'] == "1"]
    # grpahevents = game_df.pivot("event", "playkey", "vel_avg")
    # graphevents = df.pivot("event", "playkey", "pct_eff_vel_avg")
    # f, ax = plt.subplots(figsize=(30, 35))
    # sns.heatmap(graphevents, ax=ax, cmap='coolwarm', square=True, cbar=False)

    turf_df = df.loc[df['Surface'] == 'Synthetic']
    natural_df = df.loc[df['Surface'] == 'Natural']

    # turf_events = turf_df.pivot("event", "playkey", "pct_eff_vel_avg")
    # natural_events = natural_df.pivot("event", "playkey", "pct_eff_vel_avg")

    #f, ax = plt.subplots(figsize=(20,10))
    #ax = sns.boxenplot(data=turf_df, x="event", y="pct_eff_vel_avg", hue='playkey')


    # grid_kws = {"height_ratios": (.9, .05), "hspace": .01}
    # f, (ax, cbar_ax) = plt.subplots(2, gridspec_kw=grid_kws, figsize=(25, 15))
    # ax = sns.heatmap(natural_events, ax=ax,
    #                  cbar_ax=cbar_ax,
    #                  cbar_kws={"orientation": "horizontal"},
    #                  square=True, cmap='coolwarm')
    # sns_heat = sns.heatmap(grpahevents, cmap='coolwarm')
    # figure = sns_heat.get_figure()
    #f.savefig('./data/08_reporting/test_heat_syn.png')


