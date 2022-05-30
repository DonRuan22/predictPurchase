FROM continuumio/miniconda3:latest
#define the working directory of Docker container
WORKDIR /app 

#copy everything in ./actions directory (your custom actions code) to /app/actions in container
COPY ./ ./

# install dependencies
RUN pip install -r requirements.txt

#RUN mkdir output-small

RUN ls
# command to run on container start
CMD [ "python", "./app.py" ]

RUN ls

EXPOSE 5055