SELECT
    CAST("titleId" AS VARCHAR) AS tconst,
    CAST("ordering" AS INT) AS ordering,
    CAST("title" AS VARCHAR) AS title,
    CAST("region" AS VARCHAR) AS region,
    CAST("language" AS VARCHAR) AS language,
    CAST("types" AS VARCHAR) AS types,
    CAST("attributes" AS VARCHAR) AS attributes,
    CASE
        WHEN "isOriginalTitle" = 1 THEN TRUE
        ELSE FALSE
    END AS isOriginalTitle
FROM public.title_akas;

-- Define primary key constraint
ALTER TABLE public.title_akas 
ADD CONSTRAINT pk_title_akas PRIMARY KEY ("tconst");

-- Define foreign key constraint
ALTER TABLE public.title_akas
ADD CONSTRAINT fk_tconst_title_akas FOREIGN KEY ("tconst") 
REFERENCES public.title_ratings (tconst);

ALTER TABLE public.title_akas
ADD CONSTRAINT fk_ordering_title_akas FOREIGN KEY (ordering) 
REFERENCES public.title_principals (ordering);