# CS410/510 - Data Engineering Project 

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

This project was a term long (10 weeks) with checkpoints approximately every 2 to 3 weeks called 'Milestones'. You can find all the code in each directory under 'milestone-submissions' to see how it evolved. 

The project and was optional to be in a team or not I elected to do the entire project by myself. I was a Graduate Student at PSU during the duration of this project, taking the course at the 510 level.

## Challenges
1. **Improving Pub/Sub Performance:** Reduced publishing time from 1.5 hours to 5–10 seconds and subscription time from 12 hours to about 4 minutes for 300k daily records.
2. **Timing Issues with Pub/Sub:** Overcame synchronization issues in data processing to ensure timely updates and consistent data flow.
3. **Integration with Google Services:** Ensured proper use of libraries for seamless communication with Google Pub/Sub and other services.
4. **Database Management:** Effectively handled and processed large-scale data (13.5 million records, 33,000 trips) in a PostgreSQL database.
5. **System Configuration:** Set up crontabs and system services for automated tasks and smooth operation.
6. **Data Backup:** Used Google Drive as a backup solution for milestone data uploads.

## Lessons Learned
1. **Optimizing Data Processing:** Learned how to efficiently handle real-time data streams, particularly the importance of optimizing Pub/Sub for fast data publishing and subscribing.
2. **Handling Large-Scale Data:** Gained experience in managing and processing massive datasets, understanding database performance considerations, and the importance of structuring queries for scalability.
3. **Effective Use of Cloud Services:** Deepened my understanding of integrating cloud services like Google Pub/Sub and PostgreSQL, and the significance of selecting the right libraries for seamless service communication.
4. **Troubleshooting and Debugging:** Improved my troubleshooting skills by addressing synchronization issues and fine-tuning system performance, ensuring timely data updates.
5. **Automation and Reliability:** Gained insight into system automation (via crontabs and scheduled tasks) and the importance of backup strategies to maintain data integrity and reliability.
