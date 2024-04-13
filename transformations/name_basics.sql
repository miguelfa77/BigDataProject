SELECT
    CAST("nconst" AS VARCHAR) AS nconst,  
    CAST("primaryName" AS VARCHAR) AS primaryName,
    CAST("birthYear" AS SMALLINT) AS birthYear,   
    CAST("deathYear" AS SMALLINT) AS deathYear,
    CAST("primaryProfession" AS VARCHAR) AS primaryProfession,
    CAST("knownForTitles" AS VARCHAR) AS knownForTitles
FROM public.name_basics;

ALTER TABLE public.name_basics  
ADD CONSTRAINT pk_name_basics PRIMARY KEY (nconst);