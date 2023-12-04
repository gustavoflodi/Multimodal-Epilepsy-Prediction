import matplotlib.pyplot as plt
import pandas as pd


def plot_dist(sensors, data):
    fig, axes = plt.subplots(len(sensors), 2, figsize=(8, 5))

    for i, ax in enumerate(axes):
        # Boxplot
        print(sensors[i])
        ax[0].boxplot(data[sensors[i]], vert=False)
        ax[0].set_ylabel('Values')  # Set y-axis label for boxplot

        # Histogram
        ax[1].hist(data[sensors[i]], bins=20, color='skyblue', edgecolor='black')
        ax[1].set_ylabel('Frequency')  # Set y-axis label for histogram

    # Add x-axis label to the last row of subplots
    for ax in axes[-1]:
        ax.set_xlabel('Sensor Readings')  # Set x-axis label

    # Adjust layout to avoid overlap
    plt.tight_layout()

    plt.show()


def plot_dist_labeled(data, sensors):

    unique_labels = data['label'].compute().unique()

    # Initialize subplots


    for sensor in sensors:
        print(sensor)
        fig, axes = plt.subplots(len(unique_labels), 2, figsize=(10, 6))
        # Boxplot and histogram for each label
        for i, label in enumerate(unique_labels):
            # Select data for the current label
            label_data = data[data['label'] == label][sensor]

            # Boxplot
            axes[i, 0].boxplot(label_data, vert=False)
            axes[i, 0].set_ylabel('Values')  # Set y-axis label for boxplot
            axes[i, 0].set_title(f'Label {label} Boxplot')  # Set subplot title

            # Histogram
            axes[i, 1].hist(label_data, bins=10, color='skyblue', edgecolor='black')
            axes[i, 1].set_ylabel('Frequency')  # Set y-axis label for histogram
            axes[i, 1].set_title(f'Label {label} Histogram')  # Set subplot title

        # Add x-axis label to the last row of subplots
        for ax in axes[-1]:
            ax.set_xlabel('Sensor Readings')  # Set x-axis label

        # Adjust layout to avoid overlap
        plt.tight_layout()

        # Add legends
        for i, label in enumerate(unique_labels):
            axes[i, 0].legend([f'Label {label}'])
            axes[i, 1].legend([f'Label {label}'])

        plt.show()


def stats_basic(data):
    print(data.describe())
    print(data.corr())


def plot_time_series(sensors, data_2_plot, labels):
    fig, axes = plt.subplots(3, 1, sharex=True, figsize=(8, 6))
    
    for i, col in enumerate(sensors):
        axes[i].plot(data_2_plot.compute().time, data_2_plot.compute()[col], label=col)
        axes[i].set_ylabel(col)
    
    for i, label in enumerate(labels):
        seizure_start = label['startTime']
        highlight_start_ictal = seizure_start
        highlight_end_ictal = seizure_start + label['duration']

        highlight_start_preictal = seizure_start - 30*60*1000
        highlight_start_ictal = seizure_start 
        
        axes[i].axvspan(highlight_start_preictal, highlight_start_ictal, alpha=0.2, color='red')
        axes[i].axvspan(highlight_start_ictal, highlight_end_ictal, alpha=0.2, color='yellow')

    # Set common x-axis label and title
    plt.xlabel('Time')
    plt.suptitle('Three Sensors Sharing X-Axis')

    # Add legend
    axes[0].legend()
    axes[1].legend()
    axes[2].legend()

    # Display the plot
    plt.show()


def corr_plot(data, sensors):
    
    plt.figure(figsize=(8, 6))
    plt.scatter(data[sensors[0]], data[sensors[1]], c=data[sensors[2]], cmap='viridis')
    plt.xlabel(sensors[0])
    plt.ylabel(sensors[1])
    plt.title('Gráfico de Dispersão Multivariado')
    plt.colorbar(label=sensors[2])
    plt.show()
