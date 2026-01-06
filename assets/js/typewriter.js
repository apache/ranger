document$.subscribe(() => {
    const el = document.querySelector('#typewriter');
    if (!el) return;

    const instance = new Typewriter(el, {
        strings: [
            'data processing platforms',
            'Apache Spark',
            'Trino',
            'Apache Hive',
            'Apache Ozone',
            'Apache Hadoop'
        ],
        autoStart: true,
        loop: true,
    });
});
