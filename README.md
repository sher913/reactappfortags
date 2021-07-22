This is the web i will be developing for my intern, it is based on React, FASTAPI.

For now it is only local host, thus unable for anyone to test it out yet.

Succueslly created first draft for fetching data from GMS which looks like this:
![image of curl return from gms](https://user-images.githubusercontent.com/60865228/125982874-e44121ee-d17f-47be-ba17-d0cf20a526a6.png)

From this wall of Json Text, wrangled the response painstakingly to fill the data in a table format:
![iamge of data table](https://user-images.githubusercontent.com/60865228/125983337-52469335-e47a-4c45-b24c-8088159ba524.png)


If you want to explore, most of the code changes made are to these files;


Python FASTAPI:


backend/main.py

REACT APP:


frontend/App.js


Next step is to make the table editable to submit to GMS
