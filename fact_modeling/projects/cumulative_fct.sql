/*********************************************************************************************************
User Devices Activity Datelist DDL

Similarly to what was done in day 2 of the fact data modeling week, write a DDL 
statement to create a cumulating user activity table by device.

This table will be the result of joining the devices table onto the web_events table, 
so that you can get both the user_id and the browser_type.

The name of this table should be user_devices_cumulated.

The schema of this table should look like:

    user_id bigint
    browser_type varchar
    dates_active array(date)
    date date

The dates_active array should be a datelist implementation that tracks how many times 
a user has been active with a given browser_type.

Note that you can also do this using a MAP(VARCHAR, ARRAY(DATE)) type, but then you have to 
know how to manipulate the contents of those maps correctly (and then you don't include a browser_type column). 
If you use the MAP type, you'd have one row per user_id, and the keys of this MAP would be the values for browser_type, 
and the values would be the arrays of dates for which we saw activity for that user on that browser type.

Note only that, but you'll need to take care of doing the CROSS JOIN UNNEST correctly - when we did it in lab, 
we didn't do it against a MAP type, but an ARRAY type, so it exploded into rows in the way you'd expect.

Doing this by just including a browser_type column means it works almost exactly the same as what we did in lab, 
you just add an additional group by key.

The first index of the date list array should correspond to the most recent date (today's date).

*******************************************************************************************************************/