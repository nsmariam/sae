# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexionconn = sqlite3.connect("ClassicModel.sqlite")
conn = sqlite3.connect("ClassicModel.sqlite")

# Récupération du contenu de Customers avec une requête SQL
customer = pandas.read_sql_query("SELECT * FROM Customers;", conn)
print(customer)


orders = pandas.read_sql_query("SELECT * FROM Orders;", conn)
print(orders)



##Lister les clients n’ayant jamais effecuté une commande 
rq1 = pandas.read_sql_query(
    """
    SELECT c.customerNumber, c.customerName
    FROM Customers c 
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber 
    WHERE o.orderNumber is NULL;
    """,
    conn)

##Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci ;
rq2 =  pandas.read_sql_query(
    """
    SELECT 
    e.employeeNumber,
    e.firstName,
    e.lastName,
    COUNT(DISTINCT c.customerNumber) AS numberOfCustomers, 
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
    SUM(p.amount) AS totalOrderAmount
    FROM Employees e
    LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN Payments p ON o.customerNumber = p.customerNumber
    GROUP BY e.employeeNumber, e.firstName , e.lastName;
    """,
    conn)
    

#Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a ;

rq3 =  pandas.read_sql_query(
    """
    SELECT 
        of.officeCode,
        of.city,
        of.country AS officeCountry,
        COUNT(DISTINCT c.customerNumber) AS numberOfCustomers, 
        COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
        SUM(p.amount) AS totalOrderAmount,
        COUNT(DISTINCT CASE
              WHEN c.country != of.country THEN c.customerNumber
              ELSE NULL
        END) AS customersFromDifferentCountry
    FROM Offices of
        LEFT JOIN Employees e ON e.officeCode = of.officeCode
        LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber
        LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
        LEFT JOIN Payments p ON o.customerNumber = p.customerNumber
        GROUP BY of.officeCode, of.city, of.country;
       
    """,
    conn)


#Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents ;
rq4 =  pandas.read_sql_query(
    """
    SELECT 
        p.productCode,
        p.productName,
        COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
        SUM(od.quantityOrdered) AS TotalQuantity,
        COUNT(DISTINCT o.customerNumber) AS numberOfCustomers
    FROM Products p
        LEFT JOIN OrderDetails od ON p.productCode = od.productCode
        LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
        GROUP BY p.ProductCode, p.ProductName;
    
    """,
    conn)
    

# Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final ;


rq5 = pandas.read_sql_query(
    """
    SELECT
        c.country,
        COUNT(DISTINCT o.orderNumber) AS numberOfOrders, 
        SUM(p.amount) AS totalPaidAmount,
        SUM(od.priceEach * od.quantityOrdered) AS totalOrderAmount
    FROM Customers c
        LEFT JOIN Orders o ON o.customerNumber = c.customerNumber
        LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
        LEFT JOIN Payments p ON p.customerNumber = c.customerNumber
        GROUP BY c.country;  
    """,
    conn)


# On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client

rq6 = pandas.read_sql_query(
    """
    SELECT
        p.productLine,
        c.country,
        COUNT(DISTINCT o.orderNumber) AS numberOfOrders
    FROM OrderDetails od
        LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
        LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
        LEFT JOIN Products p ON od.productCode = p.productCode
    GROUP BY p.productLine, c.country;    
    """,
    conn)



# On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule 

rq7 = pandas.read_sql_query(
    """
    SELECT
        p.productLine,
        c.country,
        SUM(p.amount) AS totalPaidAmount
    FROM OrderDetails od
        LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
        LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
        LEFT JOIN Products p ON od.productCode = p.productCode
    GROUP BY p.productLine, c.country;    
    """,
    conn)



# Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach)

rq8 = pandas.read_sql_query(
    """
    SELECT
        p.productCode,
        p.productName,
        AVG(od.priceEach - p.buyPrice) AS averageMargin
    FROM OrderDetails od
        LEFT JOIN Products p ON od.productCode = p.productCode
    GROUP BY p.productCode, p.productName
    ORDER BY averageMargin DESC
    LIMIT 10
    """,
    conn)


# Lister les produits (avec le nom et le code du client) qui ont été vendus à perte :
#   Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois,
#   Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;


rq9 = pandas.read_sql_query(
    """
    SELECT
        p.productCode,
        p.productName,
        c.customerNumber,
        c.customerName,
        od.priceEach,
        p.buyPrice,
        (od.priceEach - p.buyPrice) AS loss
    FROM OrderDetails od
        LEFT JOIN Products p ON od.productCode = p.productCode
        LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
        LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
    WHERE od.priceEach < p.buyPrice
    """,
    conn)



# Lister les clients pour lesquels le montant total payé est inférieur aux montants totals des achats ;


rq10 = pandas.read_sql_query(
    """
    SELECT
        c.customerNumber,
        c.customerName,
        SUM(od.priceEach * od.quantityOrdered) AS TotalAmount,
        SUM(p.amount) AS totalPayments
    FROM Customers c
        LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
        LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
        LEFT JOIN Payments p ON c.customerNumber = p.customerNumber
    GROUP BY c.customerNumber, c.customerName
    HAVING SUM(p.amount) < SUM(od.priceEach * od.quantityOrdered)
    """,
    conn)



# Fermeture de la connexion : IMPORTANT à faire dans un cadre professionnel
conn.close()
