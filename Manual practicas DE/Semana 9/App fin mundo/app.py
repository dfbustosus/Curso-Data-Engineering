from flask import Flask, render_template, request, jsonify
import datetime

app = Flask(__name__)

# Set the fictional doomsday date (YYYY, MM, DD, HH, MM, SS)
doomsday_datetime = datetime.datetime(2025, 12, 31, 23, 59, 59)

# List to store comments
comments = []

@app.route('/')
def index():
    # Calculate the time remaining until doomsday
    current_datetime = datetime.datetime.now()
    time_remaining = doomsday_datetime - current_datetime

    # Convert the remaining time into days, hours, minutes, and seconds
    days = time_remaining.days
    hours, remainder = divmod(time_remaining.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Format the remaining time as a string
    remaining_time_str = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"

    return render_template('index.html', remaining_time=remaining_time_str, comments=comments)

@app.route('/comment', methods=['POST'])
def add_comment():
    comment = request.form.get('comment')
    if comment:
        comments.append(comment)
        return jsonify({"message": "Comment added successfully."})
    else:
        return jsonify({"error": "Invalid comment data."}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
