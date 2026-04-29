db.createUser({
    user: 'de_pipeline',
    pwd: 'de_pipeline123',
    roles: [
        { role: 'read', db: 'mydb' }
    ]
});

console.log("Created read-only user on mongo's mydb")