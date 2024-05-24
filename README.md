# CS510 (Grad) - Data Engineering Project 

## What is this project about
For this project we were tasked to do the following:

- Use data graciously provided by [TriMet](https://trimet.org/about/), hosted on an API server owned and operated by the [Computer Science Department at Portland State University](https://www.pdx.edu/engineering/).
- Create an end-to-end pipeline
    - **Gathering** data from two API: 
        - The **'Breadcrumb'** API with a set of vehicles assigned to us at the start of the term which had data for the event id, day it took place on, what time the event was recorded (in seconds) which allowed us to compute exactly what time of day it happened down to the second, the vehicles longitude and latitude. 
        - The other was the **'StopID API'** which gave us additional data points to indicate the direction, route id and which day of the week the event took place on.
    - **Transporting** it using Google Cloud and the Pub/Sub API
    - **Validations** to assure our data was correct within given boundaries.
    - **Transforming** it by adding in the exact timestamp an event took place, and the speed at which a vehicle was traveling. In my case, I also removed any vehicles going exceptionally fast (113KMH/70MPH) which just did not make sense for a public transit bus in Portland, Oregon. 
       - This also involved correctly incorporating the data from both API into the database seamlessly and soundly enough to prevent against crashes that may come up.
    - Finally, **storing** it into a pSQL database which would then be used to **visualize** with [MapBox](https://www.mapbox.com/) hosted on the VM.

This project was a term long (10 weeks) with checkpoints approximately every 2 to 3 weeks called 'Milestones'. You can find all the code in each directory under 'milestone-submissions' to see how it evolved. The project and was optional to be in a team or not I elected to do the entire project by myself.

## Challenges

## Lessons Learned

## Knowledge Gained