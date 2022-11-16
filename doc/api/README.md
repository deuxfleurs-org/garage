# Browse doc

Run in this directory:

```
python3 -m http.server 
```

And open in your browser:
  - http://localhost:8000/garage-admin-v0.html

# Validate doc

```
wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/6.1.0/openapi-generator-cli-6.1.0.jar -O openapi-generator-cli.jar
java -jar openapi-generator-cli.jar validate -i garage-admin-v0.yml
```
