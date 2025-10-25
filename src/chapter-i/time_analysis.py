import numpy as np
import json
import matplotlib.pyplot as plt
from pathlib import Path

# We read the json file with the time data
time_analysis_path = Path('time_analysis')
time_file_name = 'chapter-i.json'
time_analysis_time_file_path = time_analysis_path / time_file_name

with open(time_analysis_time_file_path, 'r') as f:
    time_data = json.loads(f.read())

# Read the json file with the system data
system_file_name = 'chapter-i-system.json'
time_analysis_system_file_path = time_analysis_path / system_file_name

with open(time_analysis_system_file_path, 'r') as f:
    system_data = json.loads(f.read())

# We will color-code each step differently
steps_data = {
    'download': {
        'color': '#1f77b4',
        'y_offset': -0.1,
        'label': f"download ({system_data['download_MB/s']:.2f} MB/s)"
    },
    'encrypt':  {
        'color': '#ff7f0e',
        'y_offset': 0.0,
        'label': f"encrypt ({system_data['cpu_count']} cores)"
    },
    'upload':{
        'color': '#7f7f7f',
        'y_offset': 0.1,
        'label': f"upload ({system_data['upload_MB/s']:.2f} MB/s)"
    },
}

plt.figure()
used_labels = set()

# We plot the data: x axis is for time in seconds (continuous), y for each file (discrete)
for file_id, file_data in time_data.items():
    for step, step_data in steps_data.items():
        label = step_data['label'] if step not in used_labels else None
        y_value = int(file_id) + step_data['y_offset']
        plt.plot(
            [file_data[step]['start'], file_data[step]['end']],
            [y_value, y_value],
            color=step_data['color'],
            label=label
        )
        used_labels.add(step)

plt.title('Time analysis for processing of files')
plt.xlabel('Time (s)')
plt.ylabel('File id')
plt.yticks(np.arange(0, plt.ylim()[1] + 1, 1))
plt.legend()
plt.grid()

# We save the plot as png for posterior inspection
plt.savefig(time_analysis_time_file_path.with_suffix('.png'), dpi=150, bbox_inches='tight')

