run_test:
	docker compose down --volumes && docker compose up --build --abort-on-container-exit
				
lint:
	ruff ./ && pylint ./src && mypy . --explicit-package-bases
