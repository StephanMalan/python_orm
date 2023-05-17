run_test:
	docker compose down --volumes && docker compose up --build --abort-on-container-exit
	# docker compose down --volumes && docker compose up --build

lint:
	ruff ./ && pylint ./src && mypy . --explicit-package-bases
