This is the web i will be developing for my intern, it is based on React, FASTAPI, yarn. The Nodejs folder is useless(old work)

Firstly, have to download this folder'datahub tagging ui' and paste in datahub's parent folder from https://github.com/linkedin/datahub

For now i am using a datahub's restendpoint on a VM, if you do not have access to that VM, you have to change the local host endpoint address

Features:
Able to use this react UI to update information on datasets within Datahub through GMS
![Image of data table](https://user-images.githubusercontent.com/60865228/131979630-40cbc3b5-2f1f-4e86-ac6f-6537acddcfaf.png)

This is Datahub's UI running on localhost
![Image of datahub UI edited](https://user-images.githubusercontent.com/60865228/131979923-ebe9efec-f11d-4da3-870b-c9a6294de6a0.png)


If you want to explore, most of the code changes made are to these files;


Python FASTAPI:
backend/main.py
backend/ingestion

REACT APP:
frontend/App.js


To run:
root folder is datahub by linkedin
Installation for first time:
    1. cd 'datahub tagging ui'
    (setup.py located in backend folder)
    2. cd backend
    3. pip install the setup.py

To run UI:
Firstly, initialize FASTAPI:
    1. cd 'datahub tagging ui'
    2. cd backend
    3. python main.py

Secondly, initialize REACT via yarn:
    1. cd 'datahub tagging ui'
    2. cd frontend
    3. yarn start