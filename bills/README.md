# About the dataset

[Link to the dataset](https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565)

# Questions

1. Find out how many orders, how many products and how many sellers are in the data ? How many products have been sold at least once? Which is the product contained in more orders?

2. How many distinct products have been sold in each day?

3. What is the average revenue of the orders ?

4. For each seller, what is the average % contribution of an order to the seller's daily quota?

    **Example**

    If `Seller_0` with `quota=250` has 3 orders:
    - Order 1: 10 products sold
    - Order 2: 8 products sold
    - Order 3: 7 products sold

    The average % contribution of orders to the seller's quota would be:
    - Order 1: 10/250 = 0.04
    - Order 2: 8/250 = 0.032
    - Order 3: 7/250 = 0.028

    Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333

5. Who are the second most selling and the least selling persons (sellers) for each product? Who are those for product with `product_id = 0`

6. Create a new column called "hashed_bill" defined as follows:
- If the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is `nbAAnllA`, you would apply hashing three times iteratively (only if the order number is even)
- If the order_id is odd: apply SHA256 hashing to the bill text
Finally, check if there are any duplicates on the new column
