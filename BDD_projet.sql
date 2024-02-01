/* RANDRIAMITANDRINA Finaritra
 * VIVIER Maude
 * M1 Informatique - Bases de données avancées
 * 
 * Fonction unify_catalog 
 * ne prend rien en paramètre
 * recrée et intègre dans la table C_ALL les données de différents catalogues
 * fait des affichages
 * ne retourne rien
 */
CREATE OR REPLACE FUNCTION unify_catalog() RETURNS VOID AS $$
DECLARE
	/* cat : nom de catalogue
	 * att_name : attribut d'un catalogue contenant name
	 * att_price : attribut d'un catalogue contenant price
	 * cursDyn : curseur dynamique
	 * requete : permet de construire une requête qui cherche le nom de l'attribut name et le nom de l'attribut price
	 * res : enregistrement resultat (pname, pprice)
	 * code : permet de récupérer le code de transformation de la table META
	 */
	cat VARCHAR;
	att_name VARCHAR;
	att_price VARCHAR;
	cursDyn REFCURSOR;
	requete VARCHAR; 
	res RECORD;
	code VARCHAR;

BEGIN
	-- Détruit la table C_ALL si elle existe
	DROP TABLE IF EXISTS C_ALL;
	-- Créer une table C_ALL où le pid est généré automatiquement
	CREATE TABLE C_ALL
	(
    	pid SERIAL PRIMARY KEY,
    	pname VARCHAR(50),
    	pprice NUMERIC(8,2)
	);
	RAISE NOTICE '___________DEBUT DU PARCOURS___________';
	-- Parcours la table META
	FOR cat IN SELECT table_name FROM meta LOOP
		cat := LOWER(cat);
		RAISE NOTICE 'Pour le catalogue : %', cat;
		/* Récupère dans le schéma de chaque catalogue
	 	 * les noms des attributs qui contiennent name
	 	 */ 
		SELECT column_name INTO att_name
		FROM information_schema.columns
		WHERE table_name = cat
		AND 
		column_name ILIKE '%name%';
		RAISE NOTICE ' avec l attribut name	: %', att_name;
		/* Récupère dans le schéma de chaque catalogue
	 	 * les noms des attributs qui contiennent price
	 	 */ 
		SELECT column_name INTO att_price
		FROM information_schema.columns 
		WHERE table_name = cat
		AND column_name ILIKE '%price%';
		RAISE NOTICE ' avec l attribut price	: %', att_price;
		/* Pour chaque table disponible dans catalog_name_price
	 	 * charge dynamiquement les données dans C_ALL, 
	 	 * à partir des noms des attributs name et price précédemment trouvés 
         * Construction de la requête du curseur dynamique
         */
        requete := 'SELECT ' 
       				|| att_name  || ' AS pname, ' 
       				|| att_price || ' AS pprice 
					FROM ' || cat;
        IF requete IS NULL THEN
			RAISE EXCEPTION 'Le catalogue % (ou un de ses attributs) de la table META, n existe pas.', cat ;
		END IF;
        OPEN cursDyn FOR EXECUTE requete;
       	FETCH cursDyn INTO res;
        LOOP
            EXIT WHEN NOT FOUND;
            RAISE NOTICE '- Un tuple trouvé est	: (%,%)', res.pname, res.pprice;
           /* Verifie dans la table META 
            * si des transformations sont à appliquer aux données
            */
            SELECT trans_code INTO code
          	FROM meta
         	WHERE table_name = UPPER(cat);  		   
			IF code IS NOT NULL THEN
           		IF code LIKE '%CAP%' then
           			-- Met en majuscule
           			res.pname := UPPER(res.pname);
           		END IF;
           		IF code LIKE '%CUR%' then
           			-- Convertit le prix (qui est en dollars), en euros 
           			res.pprice := res.pprice / 1.05;
           		END IF;
           		RAISE NOTICE '		transformé en	: (%,%)', res.pname, res.pprice;
			END IF;
			/* Insère les données dans C_ALL
			 * après avoir effectué toutes les modifications nécessaires
			 */
            INSERT INTO C_ALL (pname, pprice) VALUES (res.pname, res.pprice);
            FETCH cursDyn INTO res;
        END LOOP;
        CLOSE cursDyn;
      RAISE NOTICE '_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _';
    END LOOP;
    RAISE NOTICE '____________FIN DU PARCOURS____________';

   EXCEPTION
		-- Si une requête renvoie null, arrête le programme 
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'Aucune donnée trouvée dans la requête.';
        WHEN others then
        	RAISE EXCEPTION 'Une erreur s est produite';

END
$$ LANGUAGE plpgsql;

-- Teste la fonction unify_catalog, voir la sortie pour les affichages
SELECT unify_catalog();
