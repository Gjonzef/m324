from django.core.management.base import BaseCommand

from ledger_listener.listener import run


class Command(BaseCommand):
    help = "Runs the event-driven CDC listener."

    def handle(self, *args, **options):
        run()
