function toUnix(date) {
    return Math.floor(date.getTime() / 1000);
}

function startOfDay(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function endOfDay(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function getDateRange(option) {
    const now = new Date();
    let start, end;

    switch (option) {
        case "All Time":
            return { start: 0, end: toUnix(now) };

        case "This Week": {
            const monday = startOfDay(new Date(now));
            monday.setDate(now.getDate() - ((now.getDay() + 6) % 7)); // shift to Monday
            start = monday;
            end = endOfDay(now);
            break;
        }

        case "Last Week": {
            const monday = startOfDay(new Date(now));
            monday.setDate(now.getDate() - ((now.getDay() + 6) % 7) - 7); // last week's Monday
            start = monday;
            end = endOfDay(new Date(monday.getTime() + 6 * 86400000)); // +6 days
            break;
        }

        case "This Month":
            start = new Date(now.getFullYear(), now.getMonth(), 1);
            end = endOfDay(new Date(now.getFullYear(), now.getMonth() + 1, 0)); // last day
            break;

        case "Last Month":
            start = new Date(now.getFullYear(), now.getMonth() - 1, 1);
            end = endOfDay(new Date(now.getFullYear(), now.getMonth(), 0)); // last day prev month
            break;

        case "Last 3 Months":
            start = new Date(now.getFullYear(), now.getMonth() - 3, 1);
            end = endOfDay(new Date(now.getFullYear(), now.getMonth(), 0));
            break;

        case "Last 6 Months":
            start = new Date(now.getFullYear(), now.getMonth() - 6, 1);
            end = endOfDay(new Date(now.getFullYear(), now.getMonth(), 0));
            break;

        case "This Year":
            start = new Date(now.getFullYear(), 0, 1);
            end = endOfDay(new Date(now.getFullYear(), 11, 31));
            break;

        default:
            return null;
    }

    return { start: toUnix(start), end: toUnix(end) };
}
