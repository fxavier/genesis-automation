# Generated by Django 4.2.6 on 2023-10-11 09:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0002_alter_rendaprovenienteactividade_data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frut'),
    ]

    operations = [
        migrations.AlterField(
            model_name='rendaprovenienteactividade',
            name='data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_idade_arvore',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='rendaprovenienteactividade',
            name='data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_quantidade_arvores_machamba',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='rendaprovenienteactividade',
            name='data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_tempo_de_producao',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
