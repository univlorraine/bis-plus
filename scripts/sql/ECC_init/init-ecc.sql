-- Initialisation source ECC (SQL Server) pour test du hook ECCSourceHook.
-- Crée une base ECC_TEST + une table AGR_AGRS calquée sur SAP + quelques rôles.

IF DB_ID('ECC_TEST') IS NULL
    CREATE DATABASE ECC_TEST;
GO

USE ECC_TEST;
GO

IF OBJECT_ID('dbo.AGR_AGRS', 'U') IS NOT NULL
    DROP TABLE dbo.AGR_AGRS;
GO

-- Colonnes calquées sur la table SAP standard AGR_AGRS
-- (assignation de rôles enfants à un rôle composite).
CREATE TABLE dbo.AGR_AGRS (
    MANDT     NVARCHAR(3)  NOT NULL,
    AGR_NAME  NVARCHAR(30) NOT NULL,
    CHILD_AGR NVARCHAR(30) NOT NULL,
    CONSTRAINT PK_AGR_AGRS PRIMARY KEY (AGR_NAME, CHILD_AGR)
);
GO

INSERT INTO dbo.AGR_AGRS (MANDT, AGR_NAME, CHILD_AGR) VALUES
    ('100', 'Z_COMP_FINANCE',   'Z_SINGLE_FI_DISPLAY'),
    ('100', 'Z_COMP_FINANCE',   'Z_SINGLE_FI_POST'),
    ('100', 'Z_COMP_FINANCE',   'Z_SINGLE_FI_REPORT'),
    ('100', 'Z_COMP_HR',        'Z_SINGLE_HR_PA'),
    ('100', 'Z_COMP_HR',        'Z_SINGLE_HR_PD'),
    ('100', 'Z_COMP_LOGISTICS', 'Z_SINGLE_MM_DISPLAY'),
    ('100', 'Z_COMP_LOGISTICS', 'Z_SINGLE_SD_ORDER'),
    ('100', 'Z_COMP_ADMIN',     'Z_SINGLE_BASIS_OPS');
GO

PRINT 'Init ECC_TEST.AGR_AGRS terminé.';
SELECT COUNT(*) AS rowcount_AGR_AGRS FROM dbo.AGR_AGRS;
GO
