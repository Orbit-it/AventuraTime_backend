exports.calculateProgress = (total, completed) => {
    if (total === 0) return 0;
    return (completed / total) * 100;
  };
  