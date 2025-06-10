// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotLiquid;
using EnsureThat;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Features.Operations.ConvertData.Models;
using Microsoft.Health.Fhir.Core.Messages.ConvertData;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.TemplateManagement;
using Microsoft.Health.Fhir.TemplateManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.Fhir.Core.Features.Operations.ConvertData
{
    public class DefaultTemplateProvider : IConvertDataTemplateProvider, IDisposable
    {
        private bool _disposed = false;
        private readonly ILogger _logger;
        private readonly MemoryCache _cache;
        private readonly TemplateCollectionProviderFactory _templateCollectionProviderFactory;
        private readonly ConvertDataConfiguration _convertDataConfig;

        public DefaultTemplateProvider(
            IOptions<ConvertDataConfiguration> convertDataConfig,
            ILogger<DefaultTemplateProvider> logger)
        {
            EnsureArg.IsNotNull(convertDataConfig, nameof(convertDataConfig));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _convertDataConfig = convertDataConfig.Value;

            _logger = logger;

            // Initialize cache and template collection provider factory
            _cache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = _convertDataConfig.CacheSizeLimit,
            });

            _templateCollectionProviderFactory = new TemplateCollectionProviderFactory(_cache, Options.Create(_convertDataConfig.TemplateCollectionOptions));
            AddCustomTHl7emplatesFromFolder();
        }

        /// <summary>
        /// Replace "microsofthealth/hl7v2templates:default" in cache with custom templates from a specified .tar.gz file
        /// in FhirServer__Operations__ConvertData__CustomHl7TemplatesFile configuration. Can use path from shared volume too.
        /// Make sure the rootTemplate *.liquid files are in package root, not under a folder.
        /// </summary>
        /// <exception cref="DefaultTemplatesInitializeException">Thrown when template load fails</exception>
        private void AddCustomTHl7emplatesFromFolder()
        {
            if (string.IsNullOrWhiteSpace(_convertDataConfig.CustomHl7TemplatesFile))
            {
                _logger.LogInformation("FhirServer__Operations__ConvertData__CustomHl7TemplatesFile is not set.");

                // Do nothing
                return;
            }

            if (!File.Exists(_convertDataConfig.CustomHl7TemplatesFile))
            {
                throw new DefaultTemplatesInitializeException(
                    TemplateManagementErrorCode.InitializeDefaultTemplateFailed,
                    $"Load custom HL7 template failed. Path not found: {_convertDataConfig.CustomHl7TemplatesFile}");
            }

            _logger.LogInformation($"FhirServer__Operations__ConvertData__CustomHl7TemplatesFile is set to '{_convertDataConfig.CustomHl7TemplatesFile}.");
            var defaultHl7TemplateImageReference = "microsofthealth/hl7v2templates:default";
            var templateInfo = new DefaultTemplateInfo(DataType.Hl7v2, defaultHl7TemplateImageReference, _convertDataConfig.CustomHl7TemplatesFile);
            _templateCollectionProviderFactory.InitDefaultTemplates(templateInfo);
            if (_cache.TryGetValue(defaultHl7TemplateImageReference, out TemplateLayer template) &&
                 template.TemplateContent.Keys.Any(key => key.StartsWith("ADT_A", StringComparison.InvariantCultureIgnoreCase)))
            {
                _logger.LogInformation($"Using custom template collection '{_convertDataConfig.CustomHl7TemplatesFile}' for '{defaultHl7TemplateImageReference}'.");
            }
            else
            {
                throw new DefaultTemplatesInitializeException(
                    TemplateManagementErrorCode.ParseTemplatesFailed,
                    $"Load custom HL7 template failed. RootTemplate ADT files not found in root of package: {_convertDataConfig.CustomHl7TemplatesFile}");
            }
        }

        /// <summary>
        /// Fetch template collection from built-in archive following a default template convert request.
        /// </summary>
        /// <param name="request">The convert data request which contains template reference.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the fetch operation.</param>
        /// <returns>Template collection.</returns>
        public async Task<List<Dictionary<string, Template>>> GetTemplateCollectionAsync(ConvertDataRequest request, CancellationToken cancellationToken)
        {
            var accessToken = string.Empty;

            _logger.LogInformation("Using the default template collection for data conversion");

            try
            {
                var provider = _templateCollectionProviderFactory.CreateTemplateCollectionProvider(request.TemplateCollectionReference, accessToken);
                return await provider.GetTemplateCollectionAsync(cancellationToken);
            }
            catch (TemplateManagementException templateEx)
            {
                _logger.LogWarning(templateEx, "Template collection is invalid");
                throw new TemplateCollectionErrorException(string.Format(Core.Resources.FetchTemplateCollectionFailed, templateEx.Message), templateEx);
            }
            catch (Exception unhandledEx)
            {
                _logger.LogError(unhandledEx, "Unhandled exception: failed to get template collection");
                throw new FetchTemplateCollectionFailedException(string.Format(Core.Resources.FetchTemplateCollectionFailed, unhandledEx.Message), unhandledEx);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _cache?.Dispose();
            }

            _disposed = true;
        }
    }
}
