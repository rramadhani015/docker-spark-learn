select
    COALESCE(MAX(id), 0)
from
    eperpus_new.user_pageview;