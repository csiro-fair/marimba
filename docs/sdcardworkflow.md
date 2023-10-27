# Image data collection in the field

This example we will look at a typical workflow for multiple Baited Underwater Video Systems(BRUVS). This system has been setup for BRUVs system that use GoPro cameras. The goal of this processing system is to be ready to process that data within hours of retrieving the data. We currently have 16 BRUVs units. That's 32 Cameras to download video from! Our system works on mapping the GoPro serial number to a BRUV unit.

## Setting up the system

Prerequisites:

    1. Blank memory cards, preferably 2 per each GoPro Camera
    2. A list of camera serial numbers and there positions. To find the camera serial number look in GoPro menu/Preferences/About/Camera Info/ should be something like **C3441325501884**
    3. Install Marimba

##Basic setup (Linux)

1. create a new collection in a new directory using the bruvs template 

>```console
>(marimba-py3.10) (base) bruvman@bruv:~/data$mkdir bruvcollection 
>```

2. Create bruv Collection and add bruv pipeline to the collection 


>```console
>(marimba-py3.10) (base) bruvman@bruv:~/data$marimba new collection ./bruvcollection/ bruvs
>```

3. add a bruvs pipeline to the collection 


>```console
>(marimba-py3.10) (base) bruvman@bruv:~/data$marimba new collection ./bruvcollection/{name of voyage} bruvs
>```
