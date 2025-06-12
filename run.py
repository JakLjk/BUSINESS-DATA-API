from business_data_api.api import initialise_flask_api


def main():
    app = initialise_flask_api()
    app.run(debug=True, host="0.0.0.0", port=5004)


if __name__ == "__main__": 
    main()
   