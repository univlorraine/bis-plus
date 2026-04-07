DROP VIEW IF EXISTS splus.v_sbv_bapi_customer_getlist;
CREATE VIEW splus.v_sbv_bapi_customer_getlist AS
    SELECT kunnr,
        land1,
        name1,
        name2,
        ort01,
        pstlz,
        regio,
        sortl,
        stras,
        telf1,
        telfx,
        xcpdk,
        adrnr,
        mcod1,
        mcod2,
        mcod3,
        anred,
        aufsd,
        bahne,
        bahns,
        bbbnr,
        bbsnr,
        begru,
        brsch,
        bubkz,
        datlt,
        erdat,
        ernam,
        exabl,
        faksd,
        fiskn,
        knazk,
        knrza,
        konzs,
        ktokd,
        kukla,
        lifnr,
        lifsd,
        locco,
        loevm,
        name3,
        name4,
        niels,
        ort02,
        pfach,
        pstl2,
        counc,
        cityc,
        rpmkr,
        sperr,
        spras,
        stcd1,
        stcd2,
        stkza,
        stkzu,
        telbx,
        telf2,
        teltx,
        telx1,
        lzone,
        xzemp,
        vbund,
        stceg,
        dear1,
        dear2,
        dear3,
        dear4,
        dear5,
        gform,
        bran1,
        bran2,
        bran3,
        bran4,
        bran5,
        ekont,
        umsat,
        umjah,
        uwaer,
        jmzah,
        jmjah,
        katr1,
        katr2,
        katr3,
        katr4,
        katr5,
        katr6,
        katr7,
        katr8,
        katr9,
        katr10,
        stkzn,
        umsa1,
        txjcd,
        periv,
        abrvw,
        inspbydebi,
        inspatdebi,
        ktocd,
        pfort,
        werks,
        dtams,
        dtaws,
        duefl,
        hzuor,
        sperz,
        etikg,
        civve,
        milve,
        kdkg1,
        kdkg2,
        kdkg3,
        kdkg4,
        kdkg5,
        xknza,
        fityp,
        stcdt,
        stcd3,
        stcd4,
        stcd5,
        stcd6,
        xicms,
        xxipi,
        xsubt,
        cfopc,
        txlw1,
        txlw2,
        ccc01,
        ccc02,
        ccc03,
        ccc04,
        bonded_area_confirm,
        donate_mark,
        consolidate_invoice,
        allowance_type,
        einvoice_mode,
        cassd,
        knurl,
        j_1kfrepre,
        j_1kftbus,
        j_1kftind,
        confs,
        updat,
        uptim,
        nodel,
        dear6,
        delivery_date_rule,
        cvp_xblck,
        suframa,
        rg,
        exp,
        uf,
        rgdate,
        ric,
        rne,
        rnedate,
        cnae,
        legalnat,
        crtn,
        icmstaxpay,
        indtyp,
        tdt,
        comsize,
        decregpc,
        ph_biz_style,
        paytrsn,
        kna1_eew_cust,
        rule_exclusion,
        kna1_addr_eew_cust,
        xvsoxr_palhgt,
        xvsoxr_pal_ul,
        xvsoxr_pk_mat,
        xvsoxr_matpal,
        xvsoxr_i_no_lyr,
        xvsoxr_one_mat,
        xvsoxr_one_sort,
        xvsoxr_uld_side,
        xvsoxr_load_pref,
        xvsoxr_dpoint,
        alc,
        pmt_office,
        fee_schedule,
        duns,
        duns4,
        sam_ue_id,
        sam_eft_ind,
        psofg,
        psois,
        pson1,
        pson2,
        pson3,
        psovn,
        psotl,
        psohs,
        psost,
        psoo1,
        psoo2,
        psoo3,
        psoo4,
        psoo5,
        j_1iexcd,
        j_1iexrn,
        j_1iexrg,
        j_1iexdi,
        j_1iexco,
        j_1icstno,
        j_1ilstno,
        j_1ipanno,
        j_1iexcicu,
        aedat,
        usnam,
        j_1isern,
        j_1ipanref,
        gst_tds,
        j_3getyp,
        j_3greftyp,
        pspnr,
        coaufnr,
        j_3gagext,
        j_3gagint,
        j_3gagdumi,
        j_3gagstdi,
        lgort,
        kokrs,
        kostl,
        j_3gabglg,
        j_3gabgvg,
        j_3gabrart,
        j_3gstdmon,
        j_3gstdtag,
        j_3gtagmon,
        j_3gzugtag,
        j_3gmaschb,
        j_3gmeinsa,
        j_3gkeinsa,
        j_3gblsper,
        j_3gkleivo,
        j_3gcalid,
        j_3gvmonat,
        j_3gabrken,
        j_3glabrech,
        j_3gaabrech,
        j_3gzutvhlg,
        j_3gnegmen,
        j_3gfristlo,
        j_3geminbe,
        j_3gfmgue,
        j_3gzuschue,
        j_3gschprs,
        j_3ginvsta,
        xsapcemxdber,
        xsapcemxkvmeq
    FROM {target_schema}.kna1;

COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kunnr IS 'Numéro de client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.land1 IS 'Clé de pays';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.name1 IS 'Nom 1';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.name2 IS 'Nom 2';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ort01 IS 'Ville';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pstlz IS 'Code postal';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.regio IS 'Région (Etat, land, province, comté, etc.)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.sortl IS 'Zone de tri';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stras IS 'Rue et numéro de rue';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.telf1 IS '1er numéro de téléphone';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.telfx IS 'Numéro de télécopie';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xcpdk IS 'Code : s''agit-il d''un compte CPD ?';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.adrnr IS 'Adresse';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.mcod1 IS 'Clé de recherche pour utilisation matchcode';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.mcod2 IS 'Clé de recherche pour utilisation matchcode';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.mcod3 IS 'Clé de recherche pour utilisation matchcode';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.anred IS 'Titre de civilité';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.aufsd IS 'Blocage central de commande client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bahne IS 'Gare train express';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bahns IS 'Gare ferroviaire';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bbbnr IS 'Numéro de site international (partie 1)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bbsnr IS 'Numéro international d''exploitation (partie 2)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.begru IS 'Groupe d''autorisations';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.brsch IS 'Code branche';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bubkz IS 'Chiffre de contrôle pour numéro d''exploitation international';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.datlt IS 'N° ligne de transmission de données';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.erdat IS 'Date de création de l''enregistrement';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ernam IS 'Nom de l''utilisateur qui a créé l''objet';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.exabl IS 'Code: points de déchargement';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.faksd IS 'Blocage central d''une facture client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.fiskn IS 'Numéro de compte de la fiche avec l''adresse fiscale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.knazk IS 'Calendrier des horaires de travail du client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.knrza IS 'Numéro de compte d''un autre payeur';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.konzs IS 'Clé du groupe';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ktokd IS 'Groupe de comptes client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kukla IS 'Classification client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.lifnr IS 'Numéro de compte fournisseur';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.lifsd IS 'Blocage central de la livraison pour client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.locco IS 'Coordonnées locales';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.loevm IS 'Témoin de suppression central pour la fiche';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.name3 IS 'Nom 3';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.name4 IS 'Nom 4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.niels IS 'Zone Nielsen';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ort02 IS 'Arrondissement';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pfach IS 'Boîte postale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pstl2 IS 'Code de la boîte postale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.counc IS 'Code comté';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.cityc IS 'City Code';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.rpmkr IS 'Marché régional';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.sperr IS 'Blocage central de comptab.';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.spras IS 'Code langue';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcd1 IS 'N° SIRET';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcd2 IS 'N° SIREN';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stkza IS 'Code : partenaire soumis à la taxe d''égalisation ?';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stkzu IS 'Soumis à la TVA sur CA';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.telbx IS 'Numéro de boîte électronique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.telf2 IS '2ème numéro de téléphone';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.telx1 IS 'Numéro de télex';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.lzone IS 'Zone transport dans / vers laquelle s''effectue la livraison';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xzemp IS 'Code : payeur divergent autorisé dans la pièce ?';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.vbund IS 'N° de la société S/L du partenaire';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stceg IS 'Numéro d''identification de la TVA sur chiffres d''affaires';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear1 IS 'Code : concurrents';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear2 IS 'Code : responsable ADV';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear3 IS 'Code : intéressé';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear4 IS 'Code : client de la gamme';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear5 IS 'Code donneur d''ordre par défaut';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.gform IS 'Forme juridique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bran1 IS 'Code branche 1';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bran2 IS 'Code branche 2';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bran3 IS 'Code branche 3';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bran4 IS 'Code branche 4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.bran5 IS 'Code branche 5';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ekont IS 'Premier contact';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.umsat IS 'Chiffre d''affaires annuel';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.umjah IS 'Année pour laquelle le chiffre d''affaires a été introduite';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.uwaer IS 'Devise du chiffre d''affaires';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.jmzah IS 'Nombre de salariés de l''année';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.jmjah IS 'Année de référence du nombre de salariés';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr1 IS 'Attribut 1';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr2 IS 'Attribut 2';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr3 IS 'Attribut 3';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr4 IS 'Attribut 4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr5 IS 'Attribut 5';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr6 IS 'Attribut 6';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr7 IS 'Attribut 7';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr8 IS 'Attribut 8';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr9 IS 'Attribut 9';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.katr10 IS 'Attribut 10';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stkzn IS 'Personne physique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.umsa1 IS 'Chiffre d''affaires annuel';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.txjcd IS 'Juridiction fiscale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.periv IS 'Version d''exercice';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.abrvw IS 'Secteur de TVA';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.inspbydebi IS 'Contrôle par le client (pas de lot de contrôle)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.inspatdebi IS 'Contrôle du bon de livraison après la livraison';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ktocd IS 'Groupe de comptes de réf. pr compte CPD (au débit)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pfort IS 'Localité de la boîte postale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.werks IS 'Division';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dtams IS 'Code avis à banque centrale pour transfert sur support magn.';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dtaws IS 'Clé d''instruction pour transfert sur support magnétique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.duefl IS 'Statut reprise données dans version suiv.';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.hzuor IS 'Affectation à la hiérarchie';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.sperz IS 'Bloc.paiement';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.etikg IS 'Etiquetage Retail : groupe de clients/de divisions';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.civve IS 'Code : utilisation civile prédominante';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.milve IS 'Code :  usage principalement militaire';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kdkg1 IS 'Clients : groupe de conditions 1';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kdkg2 IS 'Clients : groupe de conditions 2';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kdkg3 IS 'Clients : groupe de conditions 3';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kdkg4 IS 'Clients : groupe de conditions 4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.kdkg5 IS 'Clients : groupe de conditions 5';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xknza IS 'Code : payeur divergent via numéro de compte';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.fityp IS 'Type de taxe';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcdt IS 'Type n° ID taxe';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcd3 IS 'Identifiant';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcd4 IS 'Numéro TVA 4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.stcd5 IS 'Numéro d''identification fiscale 5';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xicms IS 'Code : client exonéré de la taxe ICMS';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xxipi IS 'Code : client est exonéré de la taxe IPI';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.xsubt IS 'Groupe de client du calcul SubstituiÃ§ao TributÃ¡ria';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.cfopc IS 'Catégorie CFOP du client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.txlw1 IS 'Loi fiscale : ICMS';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.txlw2 IS 'Loi fiscale : IPI';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ccc01 IS 'Code : guerre chimique et biologique pour contrôle juridique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ccc02 IS 'Code de non-prolifération nucléaire pour contrôle juridique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ccc03 IS 'Code de sécurité nationale pour contrôle juridique';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.ccc04 IS 'Code : technologie missile pour autorisation légale';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.cassd IS 'Blocage contact central pour client';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.knurl IS 'Uniform resource locator';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.j_1kfrepre IS 'Nom du représentant';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.j_1kftbus IS 'Type d''activité';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.j_1kftind IS 'Type d''industrie';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.confs IS 'Statut de la confirm. de modif. (central)';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.updat IS 'Date à laquelle les modifications ont été confirmées';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.uptim IS 'Heure de la dernière confirmation de modification';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.nodel IS 'Blocage central de suppression pour fiche';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.dear6 IS 'Code: consommateur';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.duns IS 'DUNS Number';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.duns4 IS 'DUNS+4';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psofg IS 'Groupe de gestionnaires';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psois IS 'Procédure de pré-trait. compte de tiers';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pson1 IS 'Nom 1';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pson2 IS 'Nom 2';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.pson3 IS 'Nom 3';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psovn IS 'Prénom';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psotl IS 'Titre';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psohs IS 'NÂ° de rue : n''est plus utilisé à partir de 4.6B';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psost IS 'Rue : plus utilisé à partir de la version 4.6B';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psoo1 IS 'Description';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psoo2 IS 'Description';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psoo3 IS 'Description';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psoo4 IS 'Description';
COMMENT ON COLUMN splus.v_sbv_bapi_customer_getlist.psoo5 IS 'Description';