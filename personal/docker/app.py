def main():
    try:
        name = input("Enter your name: ")
        print(f"Hello, {name}!")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()