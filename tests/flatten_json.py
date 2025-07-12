json1 = {
  "user": {
    "name": "Alice",
    "location": {
      "city": "Tbilisi",
      "country": "Georgia"
    }
  }
}

json2 = {
  "user": {
    "id": 12345,
    "name": "John Doe",
    "email": "john.doe@example.com",
    "address": {
      "street": "123 Elm Street",
      "city": "Springfield",
      "zipcode": "62704"
    },
    "orders": [
      {
        "order_id": "A1001",
        "date": "2023-06-10",
        "items": [
          {
            "product": "Laptop",
            "price": 999.99,
            "quantity": 1
          },
          {
            "product": "Mouse",
            "price": 25.50,
            "quantity": 2
          }
        ]
      },
      {
        "order_id": "A1002",
        "date": "2023-08-22",
        "items": [
          {
            "product": "Keyboard",
            "price": 45.00,
            "quantity": 1
          }
        ]
      }
    ]
  }
}

def flatten_json(x):
    result = {}

    def inner(x, parent_key='', dot='.'):

        if isinstance(x, dict):
            for key in x:
                item = x[key]
                new_key = f'{parent_key}{dot}{key}'
                inner(x, parent_key=new_key)
            return
        
        elif isinstance(x, list):
            for item in x:
                inner(item)
            return
        
        elif isinstance(x, ):
            return