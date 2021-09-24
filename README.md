This is the web i will be developing for my intern, it is based on React, FASTAPI, yarn. The Nodejs folder is useless(old work)

For now i am using a datahub's restendpoint on a VM, if you do not have access to that VM, you have to change the local host endpoint address

Features:
Able to use this react UI to update information on datasets within Datahub through GMS
![Image of data table](https://user-images.githubusercontent.com/60865228/133769051-49a21991-77ed-4d6e-a2c8-81b44fc7c775.gif)

This is one of the dataset on Datahub
![Image of datahub UI edited](https://user-images.githubusercontent.com/60865228/134659624-33da907e-3782-49bf-8f6c-c892558c33b8.png)


If you want to explore, most of the code changes made are to these files;

Python FASTAPI:\
backend/main.py

REACT APP:\
frontend/App.js

root folder contains:
>/backend\
>/frontend\
>/.gitignore\
>/README.md\
>/docker-compose.yml

<br>

Installation for first time:
>1. Download Docker for whatever platform you are on
>2. Clone this repo
>3. CD to root directory
>4. type in the cmd 'docker-compose up'

<br>

You can just use cmd 'docker-compose up' everytime to start it, even if u already installed before.

