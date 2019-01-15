import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import sys

# Open file from command line
file = open(sys.argv[1], "r+")
# Create empty arrays for X values and each lines Y values
xar = []
yar1 = []
yar2 = []
yar3 = []
yar4 = []
yar5 = []

try:
    # read fist line to get each topic
    firstLine = next(file)
    # split by , to get each column heading
    x = firstLine.split(",")
    # assign column headings
    label1 = x[1]
    label2 = x[2]
    label3 = x[3]
    label4 = x[4]
    label5 = x[5]
except:
    print("File is empty")
# create null time variable
startTime = None
# set standard time format
FMT = '%H:%M:%S'
for i, line in enumerate(file):
    # for line in file extract the values in each column. First
    # column is time, second, is topic_1 and so on.
    values = line.strip().split(",")
    # rather than having time stamps, start at 0 and find time difference
    if startTime == None:
        # sets starting time
        startDate = datetime.strptime(values[0], FMT)
        startTime = timedelta(hours=startDate.hour,
                                minutes=startDate.minute,
                                seconds=startDate.second).total_seconds()
        # appends 0 since we are doing elapsed time
        xar.append(0)
    else:

        newDate = datetime.strptime(values[0], FMT)
        newTime = timedelta(hours=newDate.hour,
                                minutes=newDate.minute,
                                seconds=newDate.second).total_seconds()
        # finds difference in time
        tDelta = newTime - startTime
        # appends difference in time
        xar.append(int(tDelta))
    # appends second value(first topic value) to the first set of y values
    # if it there is no value, it appends 0
    try:
        yar1.append(float(values[1]))
    except ValueError:
        yar1.append(0)
        pass
    # appends thrid value(second topic value) to the first set of y values
    # if it there is no value, it appends 0
    try:
        yar2.append(float(values[2]))
    except ValueError:
        yar2.append(0)
        pass

    try:
        yar3.append(float(values[3]))
    except ValueError:
        yar3.append(0)
        pass

    try:
        yar4.append(float(values[4]))
    except ValueError:
        yar4.append(0)
        pass

    try:
        yar5.append(float(values[5]))
    except ValueError:
        yar5.append(0)
        pass

# configures plot, lengend, axis labels, graph labels
fig = plt.figure()
fig.suptitle('Average sentiment score per topic over time(s)')
plt.plot(xar, yar1, color='skyblue', linewidth=2)
plt.plot(xar, yar2, color='orangered', linewidth=2)
plt.plot(xar, yar3, color='mediumspringgreen', linewidth=2)
plt.plot(xar, yar4, color='gold', linewidth=2)
plt.plot(xar, yar5, color='violet', linewidth=2)
plt.xlabel('Time(s)')
plt.ylabel('Average sentiment score')
plt.legend([label1, label2, label3, label4, label5], loc='best')
plt.show()

