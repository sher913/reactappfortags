This is the web i will be developing for my intern, it is based on React, FASTAPI, yarn.

For now i am using a datahub's restendpoint on a VM, if you do not have access to that VM, you have to change the local host endpoint address

Key Features:
Able to use this react UI to update information on datasets within Datahub through GMS:

1. Access control (login with auth0)
2. Update all aspects of datasets, E.g. BrowsePath, Tags assigned to it, Description
3. Display tags assigned to datasets and their count
4. Able to edit those tags and affect the respective datasets it is assigned to
   <br>

![Image of data table](https://user-images.githubusercontent.com/60865228/140048407-c2bc2bae-21c6-4d7f-b278-44877060697f.gif)

This is one of the dataset on Datahub
![Image of datahub UI edited](https://user-images.githubusercontent.com/60865228/134659624-33da907e-3782-49bf-8f6c-c892558c33b8.png)

If you want to explore, most of the code changes made are to these files;

Python FASTAPI:\
backend/main.py

REACT APP:\
frontend/App.js

<br>

root folder contains:

> /backend\
> /frontend\
> /.gitignore\
> /README.md\
> /docker-compose.yml

<br>

Installation for first time:

> 1.  Download and install Docker for whatever platform you are on
> 2.  Clone this repo
> 3.  CD to root directory
> 4.  Edit root/backend/.FASTAPI_Varaiables.env file using any texteditor
>     > 1.Set the address for your datahub GMS endpoint:Which is your datahub address but on port :8080 <br>
>     > 2.Set your datahub account Name
> 5.  Edit root/frontend/.env file using any texteditor.
>     > 1.REACT_APP_AUTH0_DOMAIN = your auth0 domain and REACT_APP_AUTH0_CLIENT_ID = your auth0 client id. These values can be found when setting up your auth0 app <br>
>     > 2.Set the timeout value(must be more) according to how long your machine takes to fetch 'getdataset'(located in dev tools network tab) see picture below; <br>
>     > U can leave it blank first, and it will use the default value of 3000ms, once u found your value, edit it and re-install.
>     > ![Chrom dev tools network tab](https://user-images.githubusercontent.com/60865228/134888557-ee86ba13-5178-4cfd-bd2b-b6a36b895cc3.png)
> 6.  CD back to root and type in the cmd 'docker-compose up --build'

<br>

You can just use cmd 'docker-compose up', 'docker-compose up --build'(install new changes) everytime to start it, even if u already installed before.

To reinstall, just go to your docker desktop and remove the containers then remove the images and run the command again.
