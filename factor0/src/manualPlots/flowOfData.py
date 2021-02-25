#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, Ellipse, Polygon
from matplotlib.lines import Line2D

#define:
scale = 0.5
sensorWidth = 0.1*scale
sensorHeight = 0.2*scale
cloudFunctionWidth = 0.3*scale
cloudFunctionHeight = 0.2*scale

def cloudEllipse(center):
    width = 2*sensorWidth
    height = sensorHeight
    ellipse = Ellipse(center,width,height,
                      edgecolor='black',
                      facecolor='white',
                      lw=2)
    return ellipse

def gcfRectangle(center):
    xyCorner = (center[0]-0.5*cloudFunctionWidth,
                center[1]-0.5*cloudFunctionHeight)
    return Rectangle(xyCorner,cloudFunctionWidth,cloudFunctionHeight,
                     edgecolor='black',
                     facecolor='white',
                     lw=2)
    
sensor = Rectangle((0.075,0.65),sensorWidth,sensorHeight,
                   edgecolor='black',
                   facecolor='gray',
                   lw=2)
sensorCenterX = sensor.xy[0]+sensorWidth/2
sensorCenterY = sensor.xy[1]+sensorHeight/2
azureCenter = (sensorCenterX+3*sensorWidth,sensorCenterY+2*sensorHeight)
azure = cloudEllipse(azureCenter)

sensorAzurePath = [(sensorCenterX,sensorCenterY),
                   (sensorCenterX,sensorCenterY+2*sensorHeight),
                   azureCenter]
sensorAzure = Line2D(*list(zip(*sensorAzurePath)),
                     color='black',
                     zorder=0.0)

acquireDataCenter = (azureCenter[0],azureCenter[1]-4*sensorHeight)
combinedY = azureCenter[1]+acquireDataCenter[1]
acquireData = gcfRectangle(acquireDataCenter)
azureAcquire = Line2D(*list(zip(*[azureCenter,
                                  (azureCenter[0],combinedY*4/7),
                                  (azureCenter[0],combinedY*3/7),
                                  acquireDataCenter])),
                      color='black',
                      zorder=0.0,
                      marker='v',
                      markersize=15,
                      markevery=[1,2])
owmApiCenter = (azureCenter[0],azureCenter[1]-8*sensorHeight)
owmApi = cloudEllipse(owmApiCenter)
owmAcquire = Line2D(*list(zip(*[owmApiCenter,
                                (owmApiCenter[0],
                                 acquireDataCenter[1]-owmApiCenter[1]),
                                (owmApiCenter[0],
                                 acquireDataCenter[1]-3*owmApiCenter[1]),
                                acquireDataCenter])),
                    color='black',
                    zorder=0.0,
                    marker='^',
                    markersize=15,
                    markevery=[1,2])
schedulerCenter = (acquireDataCenter[0]-0.75*cloudFunctionWidth,
                   acquireDataCenter[1])
schedulerWidth = cloudFunctionWidth/16*10*scale
schedulerHeight = cloudFunctionWidth/9*10*scale
#need elliipse instead of circle due to 16:9 stretch:
scheduler = Ellipse(schedulerCenter,schedulerWidth,schedulerHeight,
                    edgecolor='black',
                    facecolor='white',
                    lw=2)
schedulerAcquire = Line2D(*list(zip(*[schedulerCenter, acquireDataCenter])),
                          color='black',
                          zorder=0.0)
dataLakeWidth = 7/16*scale
dataLakeHeight = 7/9*scale
dataLakeCenter = (acquireDataCenter[0]+2*cloudFunctionWidth,
                  acquireDataCenter[1])
dataLakeXY = (dataLakeCenter[0]-0.5*dataLakeWidth,
              dataLakeCenter[1]-0.5*dataLakeHeight)
dataLake = Rectangle(dataLakeXY,dataLakeWidth,dataLakeHeight,
                     edgecolor='black',
                     facecolor='white',
                     lw=2)
focalPoint = (dataLakeCenter[0],dataLakeCenter[1]+dataLakeHeight/3)
acquireDataLake=Line2D(*list(zip(*[acquireDataCenter,
                                   (acquireDataCenter[0]+0.2*dataLakeCenter[0],
                                    dataLakeCenter[1]),
                                   (acquireDataCenter[0]+0.3*dataLakeCenter[0],
                                    dataLakeCenter[1]),
                                   dataLakeCenter])),
                       color='black',
                       zorder=0.0,
                       marker='>',
                       markersize=15,
                       markevery=[1,2])
sparkFunctionCenter = (dataLakeCenter[0],dataLakeCenter[1]+4*sensorHeight)
sparkFunction = gcfRectangle(sparkFunctionCenter)
loopPoints = [focalPoint,
              (focalPoint[0]-dataLakeWidth,focalPoint[1]),
              (focalPoint[0]-dataLakeWidth,
               (focalPoint[1]+sparkFunctionCenter[1])/2),
              (focalPoint[0]-dataLakeWidth,sparkFunctionCenter[1]),
              sparkFunctionCenter,
              (focalPoint[0]+dataLakeWidth,sparkFunctionCenter[1]),
              (focalPoint[0]+dataLakeWidth,focalPoint[1]),
              focalPoint]
sparkDataLake = Line2D(*list(zip(*loopPoints)),
                       color='black',
                       zorder=0.0,
                       marker='^',
                       markersize=15,
                       markevery=[2])
maxLoopX = loopPoints[-2][0]
bqPts = np.array([(maxLoopX+2*sensorWidth,dataLakeCenter[1]+dataLakeHeight),
                  (maxLoopX+4*sensorWidth,dataLakeCenter[1]-dataLakeHeight),
                  (maxLoopX,dataLakeCenter[1]-dataLakeHeight)])
bigQuery = Polygon(bqPts,closed=True,
                   edgecolor='black',
                   facecolor='white',
                   linewidth=2)
dataLakeBq = Line2D(*list(zip(*[dataLakeCenter,
                                (dataLakeCenter[0]+2*dataLakeWidth/3,
                                 dataLakeCenter[1]),
                                (dataLakeCenter[0]+dataLakeWidth,
                                 dataLakeCenter[1]),
                                (bqPts[0][0],dataLakeCenter[1])])),
                    color='black',
                    zorder=0.0,
                    marker='>',
                    markersize=15,
                    markevery=[1,2])

#run:
if __name__ == '__main__':
    import sys, os
    home = os.sep.join(os.path.dirname(sys.argv[0]).split(os.sep)[:-2])
    fig = plt.figure(figsize=(16, 9))
    ax = fig.add_axes([0, 0, 1, 1])#, frameon=False)
    ax.set_xlim(0, 1), ax.set_xticks([])
    ax.set_ylim(0, 1), ax.set_yticks([])
    ax.add_patch(sensor)
    ax.text(sensorCenterX+0.6*sensorWidth,sensorCenterY,
            'IOT\nSensor',
            size=18,
            horizontalalignment='left',
            verticalalignment='center')
    ax.add_line(sensorAzure)
    ax.plot(sensorCenterX,sensorCenterY+1.2*sensorHeight,
            color='black',
            marker='^',markersize=15)
    ax.plot(sensorCenterX+sensorWidth,azureCenter[1],
            color='black',
            marker='>',markersize=15)
    ax.add_line(azureAcquire)
    ax.scatter(azureCenter[0],combinedY/2,
               edgecolors='black',facecolors='white',
               marker='^',s=200) #size specification doesn't match others
    ax.add_line(owmAcquire)
    ax.scatter(owmApiCenter[0],acquireDataCenter[1]*4/7,
               edgecolors='black',facecolors='white',
               marker='v',s=200) #size specification doesn't match others
    ax.add_line(schedulerAcquire)
    ax.add_line(acquireDataLake)
    ax.add_line(sparkDataLake)
    ax.plot(focalPoint[0]+dataLakeWidth,
            (focalPoint[1]+sparkFunctionCenter[1])/2,
            color='black',
            marker='v',markersize=15)
    ax.add_line(dataLakeBq)
    ax.add_patch(azure)
    ax.text(*azureCenter,'Azure',
            size=24,
            horizontalalignment='center',
            verticalalignment='center')
    ax.add_patch(acquireData)
    ax.text(*acquireDataCenter,'Acquire Data\nCloud Function',
            size=18,
            horizontalalignment='center',
            verticalalignment='center')
    ax.add_patch(owmApi)
    ax.text(*owmApiCenter,'OWM API',
            size=18,
            horizontalalignment='center',
            verticalalignment='center')
    ax.add_patch(scheduler)
    ax.text(schedulerCenter[0]-0.6*schedulerWidth,schedulerCenter[1],
            'Cloud\nScheduler',
            size=18,
            horizontalalignment='right',
            verticalalignment='center')
    ax.add_patch(dataLake)
    ax.text(*dataLakeCenter,'GCS\nData\nLake',
            size=48,
            horizontalalignment='center',
            verticalalignment='center')
    ax.add_patch(sparkFunction)
    ax.text(*sparkFunctionCenter,'Spark ETL\nCloud Function',
            size=18,
            horizontalalignment='center',
            verticalalignment='center')
    ax.add_patch(bigQuery)
    ax.text(bqPts[0][0],dataLakeCenter[1],'SQL\nBig\nQuery',
            size=36,
            horizontalalignment='center',
            verticalalignment='top')
    #just drawing a red point for checking:
    #ax.plot(sensorCenterX,sensorCenterY,'.r')
    
    fig.savefig(os.path.join(home,'images','flowOfData.png'),dpi=600)
