import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from datetime import datetime

# Load historical purchase and weather data
purchase_data = pd.read_csv("historical_purchases.csv")
weather_data = pd.read_csv("historical_weather.csv")

# Merge purchase and weather data based on common date column
combined_data = pd.merge(purchase_data, weather_data, on="date", how="inner")

# Convert date column to datetime format
combined_data['date'] = pd.to_datetime(combined_data['date'])

# Extract features and target variable
X = combined_data[['date', 'product_type', 'amount', 'location', 'temperature', 'humidity', 'wind_speed']]
y = combined_data['demand']

# Convert categorical variables to dummy variables
X = pd.get_dummies(X)

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions on test set
y_pred = model.predict(X_test)

# Evaluate model
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Example usage for future demand prediction
def predict_future_demand(date, product_type, amount, location, temperature, humidity, wind_speed):
    # Convert date to datetime format
    date = datetime.strptime(date, "%Y-%m-%d")
    # Create DataFrame for prediction
    data = pd.DataFrame({
        'date': [date],
        'product_type': [product_type],
        'amount': [amount],
        'location': [location],
        'temperature': [temperature],
        'humidity': [humidity],
        'wind_speed': [wind_speed]
    })
    # Convert categorical variables to dummy variables
    data = pd.get_dummies(data)
    # Make prediction
    demand_prediction = model.predict(data)
    return demand_prediction[0]

# Example usage
future_demand = predict_future_demand('2024-06-01', 'product_A', 100, 'New York', 80, 60, 10)
print(f"Predicted future demand: {future_demand}")
